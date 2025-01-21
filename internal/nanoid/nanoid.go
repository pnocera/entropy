package nanoid

import (
	"crypto/rand"
	"errors"
	"math"
)

// defaultAlphabet is the alphabet used for ID characters by default.
var defaultAlphabet = []rune("_-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

const (
	defaultSize = 21
)

// getMask generates bit mask used to obtain bits from the random bytes that are used to get index of random character
// from the alphabet. Example: if the alphabet has 6 = (110)_2 characters it is sufficient to use mask 7 = (111)_2
func getMask(alphabetSize int) int {
	for i := 1; i <= 8; i++ {
		mask := (2 << uint(i)) - 1
		if mask >= alphabetSize-1 {
			return mask
		}
	}
	return 0
}

// Generate is a low-level function to change alphabet and ID size.
func Generate(alphabet string, size int) (string, error) {
	chars := []rune(alphabet)

	if len(alphabet) == 0 || len(alphabet) > 255 {
		return "", errors.New("alphabet must not be empty and contain no more than 255 chars")
	}
	if size <= 0 {
		return "", errors.New("size must be positive integer")
	}

	mask := getMask(len(chars))
	// estimate how many random bytes we will need for the ID, we might actually need more but this is tradeoff
	// between average case and worst case
	ceilArg := 1.6 * float64(mask*size) / float64(len(alphabet))
	step := int(math.Ceil(ceilArg))

	id := make([]rune, size)
	bytes := make([]byte, step)
	for j := 0; ; {
		_, err := rand.Read(bytes)
		if err != nil {
			return "", err
		}
		for i := 0; i < step; i++ {
			currByte := bytes[i] & byte(mask)
			if currByte < byte(len(chars)) {
				id[j] = chars[currByte]
				j++
				if j == size {
					return string(id[:size]), nil
				}
			}
		}
	}
}

// MustGenerate is the same as Generate but panics on error.
func MustGenerate(alphabet string, size int) string {
	id, err := Generate(alphabet, size)
	if err != nil {
		panic(err)
	}
	return id
}

// New generates secure URL-friendly unique ID.
// Accepts optional parameter - length of the ID to be generated (21 by default).
func New(l ...int) (string, error) {
	var size int
	switch {
	case len(l) == 0:
		size = defaultSize
	case len(l) == 1:
		size = l[0]
		if size < 0 {
			return "", errors.New("negative id length")
		}
	default:
		return "", errors.New("unexpected parameter")
	}
	bytes := make([]byte, size)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	id := make([]rune, size)
	for i := 0; i < size; i++ {
		id[i] = defaultAlphabet[bytes[i]&63]
	}
	return string(id[:size]), nil
}

// Must is the same as New but panics on error.
func Must(l ...int) string {
	id, err := New(l...)
	if err != nil {
		panic(err)
	}
	return id
}

// From Amazon S3 documentation:
//Bucket naming rules
//Bucket names must be between 3 (min) and 63 (max) characters long.
//
//Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
//
//Bucket names must begin and end with a letter or number.
//
//Bucket names must not contain two adjacent periods.
//
//Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
//
//Bucket names must not start with the prefix xn--.
//
//Bucket names must not start with the prefix sthree-.
//
//Bucket names must not start with the prefix sthree-configurator.
//
//Bucket names must not start with the prefix amzn-s3-demo-.
//
//Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias names. For more information, see Using a bucket-style alias for your S3 bucket access point.
//
//Bucket names must not end with the suffix --ol-s3. This suffix is reserved for Object Lambda Access Point alias names. For more information, see How to use a bucket-style alias for your S3 bucket Object Lambda Access Point.
//
//Bucket names must not end with the suffix .mrap. This suffix is reserved for Multi-Region Access Point names. For more information, see Rules for naming Amazon S3 Multi-Region Access Points.
//
//Bucket names must not end with the suffix --x-s3. This suffix is reserved for directory buckets. For more information, see Directory bucket naming rules.
//
//Bucket names must be unique across all AWS accounts in all the AWS Regions within a partition. A partition is a grouping of Regions. AWS currently has three partitions: aws (Standard Regions), aws-cn (China Regions), and aws-us-gov (AWS GovCloud (US)).
//
//A bucket name cannot be used by another AWS account in the same partition until the bucket is deleted.
//
//Buckets used with Amazon S3 Transfer Acceleration can't have dots (.) in their names. For more information about Transfer Acceleration, see Configuring fast, secure file transfers using Amazon S3 Transfer Acceleration.
