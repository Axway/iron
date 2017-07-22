#!/bin/sh
set -e

GIT_BRANCH="$(git symbolic-ref --short HEAD)"
if [ "$GIT_BRANCH" != "master" ]; then
    echo "Not on master branch, aborting" && false
fi

$(git diff-index --quiet HEAD --) || (echo "There are uncommitted changes in the Git working directory, aborting)" && false)

ARTIFACT_ID="$(cat pom.xml | sed -n -E "s/.*<artifactId>(.*)<\/artifactId>.*/\1/p" | head -1)"
echo "Artifact id is    : $ARTIFACT_ID"

CURRENT_VERSION="$(cat pom.xml | sed -n -E "s/.*<revision>(.*)<\/revision>.*/\1/p")"
echo "Current version is: $CURRENT_VERSION"

(echo $CURRENT_VERSION | grep -Eq "^[0-9\.]+-SNAPSHOT$") || (echo "Not a SNAPSHOT version" && false)

if [ -n "$1" ]; then
    RELEASE_VERSION="$1"
    echo "Release version is: $RELEASE_VERSION (from command line parameter)"
else
    RELEASE_VERSION="$(echo $CURRENT_VERSION | sed -E "s/([0-9.]+)-SNAPSHOT/\1/")"
    echo "Release version is: $RELEASE_VERSION (automatically computed)"
fi

(echo $RELEASE_VERSION | grep -Eq "^[0-9]+\.[0-9]+\.[0-9]+$") || (echo "Not a valid release version" && false)

NEXT_VERSION="$(echo $RELEASE_VERSION | sed -E "s/([0-9]+\.[0-9]+\.)[0-9]+/\1/")$(($(echo $RELEASE_VERSION | sed -E "s/[0-9]+\.[0-9]+\.([0-9]+)/\1/")+1))-SNAPSHOT"

echo "Next version is   : $NEXT_VERSION"

TAG_NAME="$ARTIFACT_ID-$RELEASE_VERSION"

echo "Tag name is       : $TAG_NAME"
echo

echo "Updating pom.xml revision to $RELEASE_VERSION"
sed -i -b -E "s/(<revision>).*(<\/revision>)/\1$RELEASE_VERSION\2/" pom.xml
echo

echo "Committing pom.xml"
git commit -m "Release $TAG_NAME" pom.xml
echo

echo "Creating tag $TAG_NAME"
git tag -a -m "Release $TAG_NAME" $TAG_NAME
echo

echo "Updating pom.xml revision to $NEXT_VERSION"
sed -i -b -E "s/(<revision>).*(<\/revision>)/\1$NEXT_VERSION\2/" pom.xml
echo

echo "Committing pom.xml"
git commit -m "Start next development cycle on version $NEXT_VERSION" pom.xml
echo

echo "Pushing to Git repository"
git push --follow-tags

echo
echo "SUCCESS!"
