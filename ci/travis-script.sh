#!/bin/sh
set -e

echo "TRAVIS_BRANCH = $TRAVIS_BRANCH"
echo "TRAVIS_TAG = $TRAVIS_TAG"
echo "TRAVIS_PULL_REQUEST = $TRAVIS_PULL_REQUEST"

MAVEN_PROFILES="release"
MAVEN_PHASES="clean"

if [ -n "$GPG_SECRET_KEYS" -a -n "$GPG_OWNERTRUST" ]; then
    echo "Configure Maven build to sign artifact with GPG"
    MAVEN_PROFILES="$MAVEN_PROFILES,sign"
fi

if [ -n "$SONAR_TOKEN" ]; then
    echo "Configure Maven build to execute Sonarcloud analysis"
    MAVEN_PHASES="$MAVEN_PHASES org.jacoco:jacoco-maven-plugin:prepare-agent"
fi

if [ \("$TRAVIS_BRANCH" = "master" -o -n "$TRAVIS_TAG"\) -a -n "$OSSRH_USERNAME" -a -n "$GPG_SECRET_KEYS" -a -n "$GPG_OWNERTRUST" ]; then
    echo "Configure Maven build to deploy on Sonatype OSS RH"
    MAVEN_PROFILES="$MAVEN_PROFILES,ossrh"
    MAVEN_PHASES="$MAVEN_PHASES deploy"
else
    MAVEN_PHASES="$MAVEN_PHASES verify"
fi

if [ -n "$SONAR_TOKEN" ]; then
    MAVEN_PHASES="$MAVEN_PHASES sonar:sonar"
fi

mvn -B -P $MAVEN_PROFILES $MAVEN_PHASES
