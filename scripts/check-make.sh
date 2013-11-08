find src/ -iname '*.java' | sed {s/src/./g} | sort -u > SRC.tmp
cat src/Makefile | grep '.java \\' | sort -u | sed {s/\\t//g} | sed {s/"\\\\"//g} > MKE.tmp

diff --ignore-all-space SRC.tmp MKE.tmp

rm SRC.tmp
rm MKE.tmp
