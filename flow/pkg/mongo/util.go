package mongo

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"
)

func CompareServerVersions(a, b string) (int, error) {
	aMajor, aRest, _ := strings.Cut(a, ".")
	bMajor, bRest, _ := strings.Cut(b, ".")

	if majorCompare, err := compareSubVersion("major", aMajor, bMajor, a, b); err != nil || majorCompare != 0 {
		return majorCompare, err
	}

	aMinor, aPatch, _ := strings.Cut(aRest, ".")
	bMinor, bPatch, _ := strings.Cut(bRest, ".")

	if minorCompare, err := compareSubVersion("minor", aMinor, bMinor, a, b); err != nil || minorCompare != 0 {
		return minorCompare, err
	}

	return compareSubVersion("patch", aPatch, bPatch, a, b)
}

func compareSubVersion(typ, a, b, aFull, bFull string) (int, error) {
	if a == "" || b == "" {
		return 0, nil
	}

	var aNum, bNum int
	var err error

	if aNum, err = strconv.Atoi(a); err != nil {
		return 0, fmt.Errorf("cannot parse %s version %s of %s", typ, a, aFull)
	}
	if bNum, err = strconv.Atoi(b); err != nil {
		return 0, fmt.Errorf("cannot parse %s version %s of %s", typ, b, bFull)
	}

	return cmp.Compare(aNum, bNum), nil
}
