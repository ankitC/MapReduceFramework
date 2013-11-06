SRC=$(find src/ -iname '*.java' | sed {s/src/./g} | sort -u)
MKE=$(cat src/Makefile | grep '.java \\' | sort -u | sed {s/\\t//g} | sed {s/"\\\\"//g})

DIFF=$(echo "$MKE" | diff --ignore-all-space - <(echo "$SRC"))

echo "$DIFF"
