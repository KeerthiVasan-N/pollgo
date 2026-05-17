package main

import (
	"context"
	"fmt"
	"testing"

	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	libsignalPrekey "go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waAdv "go.mau.fi/whatsmeow/proto/waAdv"
	whatsmeowStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	_ "modernc.org/sqlite"
)

// syntheticBundle creates a locally-generated prekey bundle that is
// structurally valid so builder.ProcessBundle can establish a Signal session
// without any network access.
func syntheticBundle(registrationID, deviceID uint32) (*libsignalPrekey.Bundle, error) {
	preKeyPair, err := ecc.GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	signedPreKeyPair, err := ecc.GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	identityKeyPair, err := ecc.GenerateKeyPair()
	if err != nil {
		return nil, err
	}
	sig := ecc.CalculateSignature(
		identityKeyPair.PrivateKey(),
		signedPreKeyPair.PublicKey().Serialize(),
	)
	return libsignalPrekey.NewBundle(
		registrationID,
		deviceID,
		optional.NewOptionalUint32(1),
		1,
		preKeyPair.PublicKey(),
		signedPreKeyPair.PublicKey(),
		sig,
		identity.NewKey(identityKeyPair.PublicKey()),
	), nil
}

// newBenchClient creates a file-based whatsmeow client in b.TempDir() (fresh
// per benchmark invocation) and seeds Signal sessions for nDevices fake remote
// LID JIDs. It replicates the same path as prefetchAndEstablishSessions.
func newBenchClient(tb testing.TB, nDevices int) (*whatsmeow.Client, []types.JID) {
	tb.Helper()
	ctx := context.Background()

	// TempDir() is unique per benchmark invocation — no shared DB state across runs.
	dsn := fmt.Sprintf("file:%s/bench.db?_pragma=foreign_keys(1)", tb.(*testing.B).TempDir())
	container, err := sqlstore.New(ctx, "sqlite", dsn, nil)
	if err != nil {
		tb.Fatalf("sqlstore.New: %v", err)
	}
	tb.Cleanup(func() { container.Close() })
	deviceStore := container.NewDevice()

	// Give the local device a valid phone-based JID so getOwnID doesn't return empty.
	localJID := types.NewJID("19991110000", types.DefaultUserServer)
	deviceStore.ID = &localJID
	// LID is a separate field; set it so getOwnLID() also works.
	deviceStore.LID = types.NewJID("199771914563824", types.HiddenUserServer)
	// Account must satisfy all DB CHECK constraints:
	// adv_details: NOT NULL (any non-nil bytes)
	// adv_account_sig: NOT NULL, length = 64
	// adv_account_sig_key: NOT NULL, length = 32
	// adv_device_sig: NOT NULL, length = 64
	deviceStore.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}

	if err := container.PutDevice(ctx, deviceStore); err != nil {
		tb.Fatalf("PutDevice: %v", err)
	}

	client := whatsmeow.NewClient(deviceStore, nil)

	// Build Signal sessions for all remote devices using synthetic prekey bundles.
	// This replicates exactly what prefetchAndEstablishSessions does at startup.
	serializer := whatsmeowStore.SignalProtobufSerializer
	remoteJIDs := make([]types.JID, 0, nDevices)

	for i := 0; i < nDevices; i++ {
		// Use LID JIDs (HiddenUserServer) — same server as real participants in
		// LID-addressing groups. This skips the GetManyLIDsForPNs DB lookup in
		// encryptMessageForDevices, matching the actual hot path.
		jid := types.JID{
			User:   fmt.Sprintf("%018d", i+1),
			Device: uint16(i%4 + 1),
			Server: types.HiddenUserServer,
		}
		remoteJIDs = append(remoteJIDs, jid)

		bundle, err := syntheticBundle(uint32(i+1), uint32(jid.Device))
		if err != nil {
			tb.Fatalf("syntheticBundle: %v", err)
		}
		builder := session.NewBuilderFromSignal(client.Store, jid.SignalAddress(), serializer)
		if err := builder.ProcessBundle(ctx, bundle); err != nil {
			tb.Fatalf("ProcessBundle for %s: %v", jid, err)
		}
	}

	return client, remoteJIDs
}

func benchmarkEncryptMessageForDevices(b *testing.B, nDevices int) {
	ctx := context.Background()
	client, remoteJIDs := newBenchClient(b, nDevices)
	plaintext := []byte("benchmark-poll-vote-payload-1234567890")
	encAttrs := waBinary.Attrs{"decrypt-fail": "hide"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := client.DangerousInternals().EncryptMessageForDevices(
			ctx, remoteJIDs, fmt.Sprintf("BENCH%08d", i), plaintext, nil, encAttrs,
		)
		if err != nil {
			b.Fatalf("EncryptMessageForDevices: %v", err)
		}
	}
}

// BenchmarkEncryptSmallGroup — 30 members / 30 devices (personal group)
func BenchmarkEncryptSmallGroup(b *testing.B) { benchmarkEncryptMessageForDevices(b, 30) }

// BenchmarkEncryptLargeGroup — 517 members / 750 devices (EXTERRO INDIA scale)
func BenchmarkEncryptLargeGroup(b *testing.B) { benchmarkEncryptMessageForDevices(b, 750) }
