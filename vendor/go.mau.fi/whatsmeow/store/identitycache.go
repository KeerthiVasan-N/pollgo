// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package store

import (
	"context"
	"fmt"

	"go.mau.fi/util/exsync"
)

// contextKeyIdentityCache is a distinct value from contextKeySessionCache (= 0).
const contextKeyIdentityCache contextKey = 1

type identityCacheEntry struct {
	Key   [32]byte
	Found bool
	Dirty bool
}

type identityCache = exsync.Map[string, identityCacheEntry]

func getIdentityCache(ctx context.Context) *identityCache {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(contextKeyIdentityCache)
	if val == nil {
		return nil
	}
	if cache, ok := val.(*identityCache); ok {
		return cache
	}
	return nil
}

// getCachedIdentity returns the cached identity key for addr.
// ok is true only if the address was pre-loaded (found or known-absent).
// found indicates whether the identity record actually exists in the DB.
func getCachedIdentity(ctx context.Context, addr string) (key [32]byte, found, ok bool) {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return [32]byte{}, false, false
	}
	entry, exists := cache.Get(addr)
	if !exists {
		return [32]byte{}, false, false
	}
	return entry.Key, entry.Found, true
}

// putCachedIdentity writes a dirty identity entry into the cache.
// Returns false if there is no cache attached to ctx (falls back to direct DB write).
func putCachedIdentity(ctx context.Context, addr string, key [32]byte) bool {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return false
	}
	cache.Set(addr, identityCacheEntry{Key: key, Found: true, Dirty: true})
	return true
}

// WithCachedIdentities pre-fetches all identity keys for the given signal
// addresses in a single DB query and attaches an in-memory identity cache to
// the returned context. Subsequent IsTrustedIdentity / SaveIdentity calls that
// use this context will be served from the cache instead of hitting the DB.
func (device *Device) WithCachedIdentities(ctx context.Context, addresses []string) (context.Context, error) {
	if len(addresses) == 0 {
		return ctx, nil
	}
	identities, err := device.Identities.GetManyIdentities(ctx, addresses)
	if err != nil {
		return ctx, fmt.Errorf("failed to prefetch identities: %w", err)
	}
	wrapped := make(map[string]identityCacheEntry, len(addresses))
	for _, addr := range addresses {
		if key, ok := identities[addr]; ok {
			wrapped[addr] = identityCacheEntry{Key: key, Found: true}
		} else {
			wrapped[addr] = identityCacheEntry{Found: false}
		}
	}
	ctx = context.WithValue(ctx, contextKeyIdentityCache, (*identityCache)(exsync.NewMapWithData(wrapped)))
	return ctx, nil
}

// PutCachedIdentities flushes all dirty identity entries from the cache
// attached to ctx back to the DB in a single batch transaction.
func (device *Device) PutCachedIdentities(ctx context.Context) error {
	cache := getIdentityCache(ctx)
	if cache == nil {
		return nil
	}
	dirtyIdentities := make(map[string][32]byte)
	for addr, item := range cache.Iter() {
		if item.Dirty {
			dirtyIdentities[addr] = item.Key
		}
	}
	if len(dirtyIdentities) > 0 {
		if err := device.Identities.PutManyIdentities(ctx, dirtyIdentities); err != nil {
			return fmt.Errorf("failed to store cached identities: %w", err)
		}
	}
	cache.Clear()
	return nil
}
