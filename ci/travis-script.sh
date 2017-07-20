#!/bin/sh

if [ "$TRAVIS_SECURE_ENV_VARS" = "true" -a "$TRAVIS_PULL_REQUEST" = "false" ]; then
    echo
    echo "Build, test and generate Sonar report"
    mvn -B clean org.jacoco:jacoco-maven-plugin:prepare-agent package sonar:sonar || exit 1

    if [ "$TRAVIS_BRANCH" = "master" -a "$OSSRH_USERNAME" != "" ]; then
        echo
        echo "Decrypting GPG files"
        openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/pubring.gpg.enc -out ci/pubring.gpg || exit 1
        openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/secring.gpg.enc -out ci/secring.gpg || exit 1

        echo
        echo "Build, sign with GPG and deploy to OSSRH"
        mvn -B -P publish -DskipTests=true -s ci/settings.xml clean deploy || exit 1
    fi
else
    mvn -B clean verify || exit 1
fi
