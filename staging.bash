#!/bin/bash
#
# Script to create a new staging branch.  This assumes that the master
# branch is checked out and the working directory is clean.
#
# To confirm changes before checking them in, replace the -A option
# to "git add" below with -p.
#

# Update the following two variables before running this script.
VERSION=0.6.0
NEXT_VERSION=0.7.0

# Create the new branch.
git branch ${VERSION}-staging

# Update the version string to the next version number.
FROM=${VERSION}-SNAPSHOT
TO=${NEXT_VERSION}-SNAPSHOT

FILES="`grep -rl -- ${VERSION}-SNAPSHOT *`"

echo "$FILES" | xargs sed -i "s/$FROM/$TO/"
git add -A
git commit -m "[version] update master to $TO."

git push origin ${VERSION}-staging master
