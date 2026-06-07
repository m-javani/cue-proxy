// Copyright 2026 M. Javani
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// NodeCert holds the config for one node's certificate.
type NodeCert struct {
	NodeIdentity string   // e.g. "node1", "alpha" — used for filenames
	ServerNames  []string // SAN DNS names
}

// CAInfo holds the information about a generated CA
type CAInfo struct {
	Name        string
	CertPath    string
	KeyPath     string
	Certificate *x509.Certificate
	PrivateKey  *rsa.PrivateKey
}

// CreateCA generates a new Certificate Authority in the specified directory.
// Returns CAInfo with paths and parsed certificate.
func CreateCA(dir, caName string, yearsValid int, domain string) (*CAInfo, error) {
	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Generate CA key
	caKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, fmt.Errorf("generate CA key: %w", err)
	}

	// Generate serial number
	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate CA serial: %w", err)
	}

	dnsName := caName
	if domain != "" {
		dnsName = fmt.Sprintf("%s.%s", caName, domain)
	}

	// Create CA template
	caTemplate := x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: caName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Duration(yearsValid) * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
		DNSNames:              []string{dnsName},
	}

	// Create certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create CA cert: %w", err)
	}

	// Parse certificate for later use
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		return nil, fmt.Errorf("parse CA cert: %w", err)
	}

	// Write CA cert file
	caCertPath := filepath.Join(dir, fmt.Sprintf("%s_cert.pem", caName))
	if err := writePEM(caCertPath, "CERTIFICATE", caCertDER); err != nil {
		return nil, err
	}

	// Write CA key file
	caKeyPath := filepath.Join(dir, fmt.Sprintf("%s_key.pem", caName))
	if err := writeKeyPEM(caKeyPath, caKey); err != nil {
		return nil, err
	}

	return &CAInfo{
		Name:        caName,
		CertPath:    caCertPath,
		KeyPath:     caKeyPath,
		Certificate: caCert,
		PrivateKey:  caKey,
	}, nil
}

// LoadCA loads an existing CA from certificate and key files.
// Useful when you want to use a previously created CA.
func LoadCA(dir, caName string) (*CAInfo, error) {
	// Load CA certificate
	caCertPath := filepath.Join(dir, fmt.Sprintf("%s_cert.pem", caName))
	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert: %w", err)
	}

	caCertBlock, _ := pem.Decode(caCertPEM)
	if caCertBlock == nil {
		return nil, fmt.Errorf("failed to decode CA cert PEM")
	}

	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA cert: %w", err)
	}

	// Load CA key
	caKeyPath := filepath.Join(dir, fmt.Sprintf("%s_key.pem", caName))
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read CA key: %w", err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return nil, fmt.Errorf("failed to decode CA key PEM")
	}

	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}

	return &CAInfo{
		Name:        caName,
		CertPath:    caCertPath,
		KeyPath:     caKeyPath,
		Certificate: caCert,
		PrivateKey:  caKey,
	}, nil
}

// CreateNodeCert creates a certificate for a single node signed by the specified CA.
func CreateNodeCert(dir string, ca *CAInfo, node NodeCert, yearsValid int) (certPath, keyPath string, err error) {
	// Generate node key
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", fmt.Errorf("generate key for %s: %w", node.NodeIdentity, err)
	}

	// Generate serial number
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", fmt.Errorf("generate serial for %s: %w", node.NodeIdentity, err)
	}

	// Create certificate template
	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: node.NodeIdentity},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Duration(yearsValid) * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              node.ServerNames,
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// Create certificate signed by CA
	certDER, err := x509.CreateCertificate(rand.Reader, &template, ca.Certificate, &nodeKey.PublicKey, ca.PrivateKey)
	if err != nil {
		return "", "", fmt.Errorf("create cert for %s: %w", node.NodeIdentity, err)
	}

	// Write leaf certificate
	certPath = filepath.Join(dir, node.NodeIdentity+".pem")
	if err := writePEM(certPath, "CERTIFICATE", certDER); err != nil {
		return "", "", err
	}

	// Append CA certificate to leaf certificate (fullchain behavior)
	caPEM, err := os.ReadFile(ca.CertPath)
	if err != nil {
		return "", "", fmt.Errorf("read CA cert for %s: %w", node.NodeIdentity, err)
	}

	leafPEM, err := os.ReadFile(certPath)
	if err != nil {
		return "", "", fmt.Errorf("read leaf cert for %s: %w", node.NodeIdentity, err)
	}

	fullchain := append(leafPEM, caPEM...)
	if err := os.WriteFile(certPath, fullchain, 0644); err != nil {
		return "", "", fmt.Errorf("write fullchain for %s: %w", node.NodeIdentity, err)
	}

	// Write private key
	keyPath = filepath.Join(dir, node.NodeIdentity+"_key.pem")
	if err := writeKeyPEM(keyPath, nodeKey); err != nil {
		return "", "", fmt.Errorf("write key for %s: %w", node.NodeIdentity, err)
	}

	return certPath, keyPath, nil
}

// CreateNodeCertWithCAFile is a convenience function that loads the CA from files
// and creates a node certificate in one call.
func CreateNodeCertWithCAFile(dir, caName string, node NodeCert, yearsValid int) (certPath, keyPath string, err error) {
	// Load the CA
	ca, err := LoadCA(dir, caName)
	if err != nil {
		return "", "", fmt.Errorf("load CA: %w", err)
	}

	// Create node certificate
	return CreateNodeCert(dir, ca, node, yearsValid)
}

// Helper functions
func writePEM(path, blockType string, der []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: blockType, Bytes: der})
}

func writeKeyPEM(path string, key *rsa.PrivateKey) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
