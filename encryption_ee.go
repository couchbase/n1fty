//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package n1fty

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"math/rand"

	gocbcrypto "github.com/couchbase/gocbcrypto"
)

var n1ftyEncryptionContext = []byte("n1fty")

const (
	aes256gcmCipher = "AES-256-GCM"
	noncelength     = 12
)

// make a writer callback for the given cipher, key and context
func MakeWriterCallback(cipher string, key []byte, context string,
) (func(data []byte) []byte, error) {

	switch cipher {
	case aes256gcmCipher:
		// generate a cipher block for AES-256-GCM encryption using the provided key and context
		aesgcm, err := generateAES256GCMCipherBlock(key, context)
		if err != nil {
			return nil, err
		}

		// generate a random nonce for encryption. The nonce will be
		// incremented for each encryption operation ensuring uniqueness
		nonce := make([]byte, noncelength)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("failed to generate random nonce: %w", err)
		}

		writerCallback := func(data []byte) []byte {
			// increment the nonce for each encryption operation to ensure uniqueness
			for i := len(nonce) - 1; i >= 0; i-- {
				if nonce[i] < 255 {
					nonce[i]++
					break
				}
				nonce[i] = 0
			}
			// encrypt the data using AES-256-GCM with the generated nonce
			ciphertext := aesgcm.Seal(nil, nonce, data, nil)
			// append the nonce to the end of the ciphertext for use in decryption
			result := append(ciphertext, nonce...)
			return result
		}

		return writerCallback, nil
	default:
		return func(data []byte) []byte {
			return data
		}, nil
	}
}

// make a reader callback for the given cipher, key and context
func MakeReaderCallback(cipher string, key []byte, context string,
) (func(data []byte) ([]byte, error), error) {

	switch cipher {
	case aes256gcmCipher:
		// generate a cipher block for AES-256-GCM encryption using the provided key and context
		aesgcm, err := generateAES256GCMCipherBlock(key, context)
		if err != nil {
			return nil, err
		}

		readerCallback := func(data []byte) ([]byte, error) {
			if len(data) < noncelength {
				return nil, fmt.Errorf("ciphertext too short")
			}

			// extract the nonce from the end of the data
			nonce := data[len(data)-noncelength:]
			ciphertext := data[:len(data)-noncelength]

			// decrypt the data using AES-256-GCM with the extracted nonce
			plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt data: %w", err)
			}

			return plaintext, nil
		}

		return readerCallback, nil
	default:
		return func(data []byte) ([]byte, error) {
			return data, nil
		}, nil
	}
}

// generate a cipher block for AES-256-GCM encryption using the provided key and context
func generateAES256GCMCipherBlock(key []byte,
	context string) (cipher.AEAD, error) {
	// derive a 256-bit key from the provided key and context using OpenSSL's KBKDF
	derivedKey := make([]byte, 32)
	derivedKey, err := gocbcrypto.OpenSSLKBKDFDeriveKey(key, n1ftyEncryptionContext,
		[]byte(context), derivedKey, "SHA2-256", "")
	if err != nil {
		return nil, err
	}

	// create a new AES cipher block using the derived key
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// create a new GCM cipher mode instance using the AES block cipher
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES GCM: %w", err)
	}

	return aesgcm, nil
}
