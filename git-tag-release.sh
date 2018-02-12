#!/bin/sh
set -e

GIT_URL=https://github.com/Axway/iron.git

(git diff-index --quiet HEAD --) || (echo "There are uncommitted changes in the Git working directory, aborting)" && false)
(git symbolic-ref --short HEAD)  || (echo "The working copy is in a detached HEAD state, aborting)" && false)

GIT_LOCAL_BRANCH="$(git symbolic-ref --short HEAD)"

echo "Local branch is $GIT_LOCAL_BRANCH"

GIT_REMOTE_NAME=origin
GIT_REMOTE_URL="$(git remote get-url $GIT_REMOTE_NAME)"
if [ "$GIT_REMOTE_URL" != "$GIT_URL" ]; then
    GIT_REMOTE_NAME=upstream
    GIT_REMOTE_URL="$(git remote get-url $GIT_REMOTE_NAME)"
    if [ "$GIT_REMOTE_URL" != "$GIT_URL" ]; then
        echo "No remote match URL $GIT_URL"
        exit 1
    fi
fi

echo "Found URL $GIT_URL on remote $GIT_REMOTE_NAME"

echo "Fetch $GIT_REMOTE_NAME"
git fetch $GIT_REMOTE_NAME

echo "Checkout $GIT_REMOTE_NAME/master"
git checkout $GIT_REMOTE_NAME/master

ARTIFACT_ID="$(sed -n -E "s/.*<artifactId>(.*)<\/artifactId>.*/\1/p" pom.xml | head -1)"
echo "Artifact id is    : $ARTIFACT_ID"

CURRENT_VERSION="$(sed -n -E "s/.*<revision>(.*)<\/revision>.*/\1/p" pom.xml)"
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

NEXT_VERSION="$(echo $RELEASE_VERSION | sed -E "s/([0-9]+\.)[0-9]+\.[0-9]+/\1/")$(($(echo $RELEASE_VERSION | sed -E "s/[0-9]+\.([0-9]+)\.[0-9]+/\1/")+1)).0-SNAPSHOT"

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

echo "Verifying build"
mvn -B -P release clean verify
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
git push --follow-tags upstream HEAD:master

echo "Going back to local branch $GIT_LOCAL_BRANCH"
git checkout $GIT_LOCAL_BRANCH

echo
echo "SUCCESS!"
