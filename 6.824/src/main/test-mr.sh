#!/bin/bash
here=$(dirname "$0")
[[ "$here" = /* ]] || here="$PWD/$here"
#export GOPATH="$here/../../"
echo ""
echo "==> Part I"
go test -run Sequential 6.824/mapreduce
echo ""
echo "==> Part II"
(cd "$here" && sh ./test-wc.sh > /dev/null)
echo ""
echo "==> Part III"
go test -run TestBasic 6.824/mapreduce
echo ""
echo "==> Part IV"
go test -run Failure 6.824/mapreduce
echo ""
echo "==> Part V (challenge)"
(cd "$here" && sh ./test-ii.sh > /dev/null)

rm "$here"/mrtmp.* "$here"/diff.out
