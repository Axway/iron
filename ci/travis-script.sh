#!/bin/sh
set -e

echo "TRAVIS_BRANCH = $TRAVIS_BRANCH"
echo "TRAVIS_TAG = $TRAVIS_TAG"
echo "TRAVIS_PULL_REQUEST = $TRAVIS_PULL_REQUEST"

if [ "$TRAVIS_SECURE_ENV_VARS" = "true" -a "$TRAVIS_PULL_REQUEST" = "false" -a "$SONAR_TOKEN" != "" -a "$GPG_KEYNAME" != "" ]; then
    echo
    echo "Build, test and generate Sonar report"
    mvn -B clean org.jacoco:jacoco-maven-plugin:prepare-agent package sonar:sonar

    echo
    echo "Decrypting GPG files"
    openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/pubring.gpg.enc -out ci/pubring.gpg
    openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/secring.gpg.enc -out ci/secring.gpg

    if [ \("$TRAVIS_BRANCH" = "master" -o "$TRAVIS_TAG" != ""\) -a "$OSSRH_USERNAME" != "" ]; then
        echo
        echo "Build, sign with GPG and deploy to OSSRH"
        mvn -B -P release -DskipTests=true clean deploy
    else
        echo
        echo "Build and sign with GPG"
        mvn -B -P release -DskipTests=true clean verify
    fi
else
    echo
    echo "Build and verify"
    mvn -B clean verify
fi
