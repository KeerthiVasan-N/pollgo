package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/whatsmeow"
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	whatsmeowStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

var (
	voteOption      = 1 // default vote option (1-based)
	voteSearchQuery string
	voteOptionMu    sync.Mutex
)

// prefetchAndEstablishSessions fetches prekey bundles AND processes them
// into real Signal sessions so that SendMessage finds warm sessions.
// Only establishes sessions for devices that don't already have one.
func prefetchAndEstablishSessions(client *whatsmeow.Client, ctx context.Context, deviceJIDs []types.JID) (established int) {
	if len(deviceJIDs) == 0 {
		return 0
	}

	// Filter out devices that already have a session — don't overwrite them
	var needSession []types.JID
	for _, jid := range deviceJIDs {
		hasSession, err := client.Store.ContainsSession(ctx, jid.SignalAddress())
		if err != nil {
			log.Printf("[PREFETCH] Session check error for %s: %v\n", jid, err)
			continue
		}
		if !hasSession {
			needSession = append(needSession, jid)
		}
	}

	if len(needSession) == 0 {
		log.Printf("[PREFETCH] All %d devices already have sessions, skipping\n", len(deviceJIDs))
		return 0
	}
	log.Printf("[PREFETCH] %d/%d devices need sessions, fetching prekeys...\n", len(needSession), len(deviceJIDs))

	bundles := client.DangerousInternals().FetchPreKeysNoError(ctx, needSession)
	serializer := whatsmeowStore.SignalProtobufSerializer
	for jid, bundle := range bundles {
		if bundle == nil {
			continue
		}
		builder := session.NewBuilderFromSignal(client.Store, jid.SignalAddress(), serializer)
		if err := builder.ProcessBundle(ctx, bundle); err != nil {
			log.Printf("[PREFETCH] Session error for %s: %v\n", jid, err)
			continue
		}
		established++
	}
	return established
}

// prefetchTargetGroup warms Signal sessions for the target group
const targetGroupName = "EXTERRO INDIA"

func prefetchTargetGroup(client *whatsmeow.Client, ctx context.Context) {
	groups, err := client.GetJoinedGroups(ctx)
	if err != nil {
		log.Printf("[PREFETCH] Failed to get joined groups: %v\n", err)
		return
	}

	var target *types.GroupInfo
	for _, g := range groups {
		if strings.EqualFold(g.Name, targetGroupName) {
			target = g
			break
		}
	}
	if target == nil {
		log.Printf("[PREFETCH] Group '%s' not found\n", targetGroupName)
		return
	}

	var memberJIDs []types.JID
	for _, p := range target.Participants {
		memberJIDs = append(memberJIDs, p.JID)
	}
	deviceJIDs, err := client.GetUserDevices(ctx, memberJIDs)
	if err != nil {
		log.Printf("[PREFETCH] Failed to get devices for '%s': %v\n", target.Name, err)
		return
	}
	start := time.Now()
	established := prefetchAndEstablishSessions(client, ctx, deviceJIDs)
	log.Printf("[PREFETCH] Group '%s': established %d sessions (%d members, %d devices) in %v\n",
		target.Name, established, len(target.Participants), len(deviceJIDs), time.Since(start))
}

func extractText(msg *waE2E.Message) string {
	if msg.GetConversation() != "" {
		return msg.GetConversation()
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	if img := msg.GetImageMessage(); img != nil {
		return img.GetCaption()
	}
	if vid := msg.GetVideoMessage(); vid != nil {
		return vid.GetCaption()
	}
	if doc := msg.GetDocumentMessage(); doc != nil {
		return doc.GetCaption()
	}
	if buttons := msg.GetButtonsResponseMessage(); buttons != nil {
		return buttons.GetSelectedDisplayText()
	}
	return ""
}

func main() {
	// Initialize context for library calls
	ctx := context.Background()

	// Initialize database for session storage
	// We pass ctx as the first argument as required by the latest API
	log.Println("Initializing database for session storage...")
	container, err := sqlstore.New(ctx, "sqlite", "file:session.db?_pragma=foreign_keys(1)&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)", nil)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	log.Println("Getting first device...")
	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		log.Fatalf("Failed to get first device: %v", err)
	}

	log.Println("Creating WhatsApp client...")
	client := whatsmeow.NewClient(deviceStore, nil)
	client.UseRetryMessageStore = true

	log.Println("Adding event handler...")
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Connected:
			_ = v // suppress unused
			log.Println("[CONNECTED] Client connected, starting background prefetch for EXTERRO INDIA...")
			go func() {
				time.Sleep(2 * time.Second)
				prefetchTargetGroup(client, ctx)

				// Re-warm every 10 minutes to catch new members/devices
				ticker := time.NewTicker(10 * time.Minute)
				defer ticker.Stop()
				for range ticker.C {
					log.Println("[PREFETCH] Periodic re-warm starting...")
					prefetchTargetGroup(client, ctx)
				}
			}()
		case *events.Message:
			msg := v.Message

			// Handle vote-N and votefor-text commands from self
			if v.Info.IsFromMe {
				text := strings.TrimSpace(extractText(msg))
				lowerText := strings.ToLower(text)
				if strings.HasPrefix(lowerText, "vote-") {
					numStr := strings.TrimPrefix(lowerText, "vote-")
					num, err := strconv.Atoi(numStr)
					if err == nil && num >= 1 {
						voteOptionMu.Lock()
						voteOption = num
						voteSearchQuery = ""
						voteOptionMu.Unlock()
						log.Printf("[CONFIG] Vote option changed to %d\n", num)
					} else {
						log.Printf("[CONFIG] Invalid vote command: %s\n", text)
					}
					return
				}

				if strings.HasPrefix(lowerText, "votefor-") {
					query := strings.TrimSpace(strings.TrimPrefix(lowerText, "votefor-"))
					if query == "" {
						voteOptionMu.Lock()
						voteSearchQuery = ""
						voteOptionMu.Unlock()
						log.Printf("[CONFIG] Cleared votefor query\n")
					} else {
						voteOptionMu.Lock()
						voteSearchQuery = query
						voteOptionMu.Unlock()
						log.Printf("[CONFIG] Set votefor query to '%s'\n", query)
					}
					return
				}

				// Handle prefetch-GroupName command from self
				if strings.HasPrefix(lowerText, "prefetch-") {
					groupName := strings.TrimPrefix(text, text[:len("prefetch-")])
					go func(name string) {
						log.Printf("[PREFETCH] Looking for group: %s\n", name)

						// Get all joined groups
						groups, err := client.GetJoinedGroups(ctx)
						if err != nil {
							log.Printf("[PREFETCH] Failed to get joined groups: %v\n", err)
							return
						}

						// Find the group by name (case-insensitive)
						var targetGroup *types.GroupInfo
						for _, g := range groups {
							if strings.EqualFold(g.Name, name) {
								targetGroup = g
								break
							}
						}

						if targetGroup == nil {
							log.Printf("[PREFETCH] Group '%s' not found. Available groups:\n", name)
							for _, g := range groups {
								log.Printf("[PREFETCH]   - %s\n", g.Name)
							}
							return
						}

						log.Printf("[PREFETCH] Found group '%s' with %d participants\n", targetGroup.Name, len(targetGroup.Participants))

						// Collect participant JIDs
						var memberJIDs []types.JID
						for _, p := range targetGroup.Participants {
							memberJIDs = append(memberJIDs, p.JID)
						}

						// Resolve to device JIDs
						log.Printf("[PREFETCH] Resolving device JIDs for %d members...\n", len(memberJIDs))
						deviceJIDs, err := client.GetUserDevices(ctx, memberJIDs)
						if err != nil {
							log.Printf("[PREFETCH] Failed to get user devices: %v\n", err)
							return
						}
						log.Printf("[PREFETCH] Found %d devices, establishing sessions...\n", len(deviceJIDs))

						// Fetch prekeys AND establish Signal sessions
						start := time.Now()
						established := prefetchAndEstablishSessions(client, ctx, deviceJIDs)
						log.Printf("[PREFETCH] Done! Established %d sessions for %d devices in group '%s' (%v)\n",
							established, len(deviceJIDs), name, time.Since(start))
					}(groupName)
					return
				}
			}

			// Try to extract poll creation options from any version
			var pollName string
			var pollOptions []*waE2E.PollCreationMessage_Option

			if pc := msg.GetPollCreationMessage(); pc != nil {
				pollName = pc.GetName()
				pollOptions = pc.GetOptions()
			} else if pc2 := msg.GetPollCreationMessageV2(); pc2 != nil {
				pollName = pc2.GetName()
				pollOptions = pc2.GetOptions()
			} else if pc3 := msg.GetPollCreationMessageV3(); pc3 != nil {
				pollName = pc3.GetName()
				pollOptions = pc3.GetOptions()
			}

			if pollOptions != nil {
				log.Printf("Received poll from %s in chat %s: %s\n", v.Info.Sender, v.Info.Chat, pollName)
				log.Printf("Poll message ID: %s, IsGroup: %v\n", v.Info.ID, v.Info.IsGroup)
				for i, opt := range pollOptions {
					log.Printf("  Option %d: %s\n", i+1, opt.GetOptionName())
				}

				voteOptionMu.Lock()
				selectedIdx := voteOption - 1
				query := voteSearchQuery
				voteOptionMu.Unlock()

				if query != "" {
					found := false
					for i, opt := range pollOptions {
						if strings.Contains(strings.ToLower(opt.GetOptionName()), query) {
							selectedIdx = i
							found = true
							break
						}
					}
					if found {
						log.Printf("[CONFIG] votefor query matched option %d\n", selectedIdx+1)
					} else {
						log.Printf("[CONFIG] votefor query '%s' did not match any option\n", query)
					}
				}

				if selectedIdx < 0 || selectedIdx >= len(pollOptions) {
					log.Printf("[WARN] Vote option %d is out of range (poll has %d options), defaulting to option 1\n", selectedIdx+1, len(pollOptions))
					selectedIdx = 0
				}

				if len(pollOptions) > 0 {
					go func(info events.Message, optName string, optNum int) {
						start := time.Now()
						log.Printf("Auto-voting for option %d: %s\n", optNum, optName)

						// Workaround: EncryptPollVote uses getOwnID() (phone number JID)
						// but should use getOwnLID() (LID) for proper encryption.
						// We manually encrypt with the correct LID using DangerousInternals.
						voteMsg := &waE2E.PollVoteMessage{
							SelectedOptions: whatsmeow.HashPollOptions([]string{optName}),
						}
						plaintext, err := proto.Marshal(voteMsg)
						if err != nil {
							log.Printf("Failed to marshal poll vote: %v\n", err)
							return
						}

						internals := client.DangerousInternals()
						ownLID := internals.GetOwnLID()
						log.Printf("Using LID for vote encryption: %s\n", ownLID)

						ciphertext, iv, err := internals.EncryptMsgSecret(
							ctx, ownLID,
							info.Info.Chat, info.Info.Sender, info.Info.ID,
							whatsmeow.EncSecretPollVote, plaintext,
						)
						if err != nil {
							log.Printf("Failed to encrypt poll vote: %v\n", err)
							return
						}

						// Build the poll creation message key
						creationKey := &waCommon.MessageKey{
							RemoteJID: proto.String(info.Info.Chat.String()),
							FromMe:    proto.Bool(info.Info.IsFromMe),
							ID:        proto.String(string(info.Info.ID)),
						}
						if info.Info.IsGroup {
							creationKey.Participant = proto.String(info.Info.Sender.String())
						}

						pollUpdateMsg := &waE2E.Message{
							PollUpdateMessage: &waE2E.PollUpdateMessage{
								PollCreationMessageKey: creationKey,
								Vote: &waE2E.PollEncValue{
									EncPayload: ciphertext,
									EncIV:      iv,
								},
								SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
							},
						}

						sendStart := time.Now()
						resp, err := client.SendMessage(ctx, info.Info.Chat, pollUpdateMsg)
						if err != nil {
							log.Printf("Failed to send poll vote: %v\n", err)
						} else {
							log.Printf("Successfully voted for option %d in %v (total %v)! Response: ID=%s, Timestamp=%v\n",
								optNum, time.Since(sendStart), time.Since(start), resp.ID, resp.Timestamp)
						}
					}(*v, pollOptions[selectedIdx].GetOptionName(), selectedIdx+1)
				}
			} else {
				// Debug: log what message type we received
				msgType := "unknown"
				if msg.GetConversation() != "" {
					msgType = "text"
				} else if msg.GetImageMessage() != nil {
					msgType = "image"
				} else if msg.GetVideoMessage() != nil {
					msgType = "video"
				} else if msg.GetPollUpdateMessage() != nil {
					msgType = "poll_update"
				}
				log.Printf("Received %s message from %s: %s\n", msgType, v.Info.Sender.User, msg.GetConversation())
				_ = fmt.Sprintf("%v", msg) // debug helper
			}
		}
	})

	log.Println("Connecting to WhatsApp...")
	if client.Store.ID == nil {
		log.Println("No previous session found. Generating QR code...")
		qrChan, _ := client.GetQRChannel(ctx)
		err = client.Connect()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		go func() {
			for evt := range qrChan {
				if evt.Event == "code" {
					log.Println("Scan the following QR code to log in:")
					qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				} else {
					log.Println("QR Event:", evt.Event)
				}
			}
		}()
	} else {
		log.Println("Found existing session. Connecting directly...")
		err = client.Connect()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
	}

	log.Println("Client connected. Waiting for interrupt signal to disconnect...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Println("Interrupt signal received. Disconnecting client...")
	client.Disconnect()
	log.Println("Client disconnected. Exiting.")
}
