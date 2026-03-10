//go:build boringcrypto

package milvus

/*
#include <openssl/conf.h>

static int enableOpenSSLFIPS() {
	return OSSL_LIB_CTX_load_config(NULL, "/milvus/configs/ssl/openssl-fips.cnf");
}
*/
import "C"

import (
	"crypto/boring"
	"log"
	"sync"
)

func boringEnabled() bool {
	return boring.Enabled()
}

var fipsOnce sync.Once

func maybeEnableOpenSSLFIPS() {
	fipsOnce.Do(func() {
		if C.enableOpenSSLFIPS() != 1 {
			log.Println("Failed to load OpenSSL FIPS config")
			return
		}
		log.Println("OpenSSL FIPS mode enabled")
	})
}
