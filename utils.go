package d1

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// BytesToUnicodeEscapes 将 []byte 转换为 Unicode 转义序列字符串
func BytesToUnicodeEscapes(b []byte) string {
	var sb strings.Builder
	for _, v := range b {
		sb.WriteString(fmt.Sprintf("\\u%04X", v))
	}
	return sb.String()
}

func IsFullyUnicodeEscaped(s string) bool {
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '\\' && s[i+1] == 'u' {
			// 检查后续四个字符是否都是十六进制数字
			if i+5 > len(s) || !isHex(s[i+2:i+6]) {
				return false
			}
			i += 6 // 跳过已检查的 Unicode 转义序列
		} else {
			return false
		}
	}
	return true
}

// isHex 检查字符串是否都是十六进制数字
func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

// UnescapeUnicode 将包含Unicode转义序列的字符串转换为对应的[]byte。
func UnescapeUnicode(s string) ([]byte, error) {
	var buf bytes.Buffer
	i := 0
	for i < len(s) {
		if i+1 < len(s) && s[i] == '\\' && s[i+1] == 'u' {
			// 检查后续四个字符是否都是十六进制数字
			if i+5 > len(s) || isHex(s[i+2:i+6]) {
				r, err := strconv.ParseInt(s[i+2:i+6], 16, 32)
				if err != nil {
					return nil, fmt.Errorf("invalid unicode escape sequence at position %d: %v", i, err)
				}
				buf.WriteByte(byte(r))
			}
			i += 6 // 跳过已检查的 Unicode 转义序列
		} else {
			continue
		}
	}
	return buf.Bytes(), nil
}
