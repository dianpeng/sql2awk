# -----------------------------------------------------
# The code is heavily modified from https://github.com/patsie75/awk-base64
# -----------------------------------------------------
function base64_encode(str,   s, len, v1,v2,v3, u, a1,a2,a3,a4, nl) {
  out = ""
  ## process whole document
  while (str) {
    # get next three characters
    s = substr(str, 1, 3)
    str = substr(str, 4)

    len = length(s)

    ## three character ascii/byte values
    v1 = base64_ord[substr(s,1,1)]
    v2 = base64_ord[substr(s,2,1)]
    v3 = base64_ord[substr(s,3,1)]

    ## convert 3x 8-bit into 24-bit value
    u = v1 * 65536 + v2 * 256 + v3

    # convert into 4x 6-bit values
    a1 = int(u/262144) % 64
    a2 = int(u/4096) % 64
    a3 = int(u/64) % 64
    a4 = u % 64

    ## add four characters (pad if len < 3)
    out = out substr(base64_b64, a1+1, 1) substr(base64_b64, a2+1, 1)
    out = out ((len < 2) ? "=" : substr(base64_b64, a3+1, 1))
    out = out ((len < 3) ? "=" : substr(base64_b64, a4+1, 1))

    ## add a newline every 19*4 (76) characters
    if (++nl == 19) {
      nl = 0
      out = out "\n"
    }
  }
  return (nl ? out "\n" : out);
}

function base64_decode(str,    out, c1,c2,c3,c4, i1,i2,i3,i4, u, v1,v2,v3) {
  out = ""

  ## remove any newlines and spaces
  gsub(/[\n ]/, "", str)

  ## process whole document
  while (str) {
    # get next four characters
    c1 = substr(str,1,1)
    c2 = substr(str,2,1)
    c3 = substr(str,3,1)
    c4 = substr(str,4,1)

    str = substr(str, 5)

    ## get index/value of each character
    i1 = index(base64_b64, c1)-1
    i2 = index(base64_b64, c2)-1
    i3 = (c3 != "=") ? index(base64_b64, c3)-1 : 0
    i4 = (c4 != "=") ? index(base64_b64, c4)-1 : 0

    ## convert 4 * 6-bit into 24-bit
    u = i1 * 262144 + i2 * 4096 + i3 * 64 + i4

    ## convert 24-bit into 3 * 8-bits
    v1 = int(u / 65536) % 256
    v2 = int(u / 256) % 256
    v3 = u % 256

    ## print result
    out = out sprintf("%c", v1)
    if (c3 != "=") out = out sprintf("%c", v2)
    if (c4 != "=") out = out sprintf("%c", v3)
  }

  return out;
}

# called by the embedder during global setup
function base64_setup(i) {
  base64_b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  for (i=0; i<256; i++) base64_ord[sprintf("%c",i)] = i;
}
