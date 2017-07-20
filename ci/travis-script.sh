#!/bin/sh

if [ "$TRAVIS_SECURE_ENV_VARS" = "true" ]; then
    echo
    echo "Decrypting GPG files"
    openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/pubring.gpg.enc -out ci/pubring.gpg || exit 1
    openssl aes-256-cbc -d -k "$OPENSSL_PASSPHRASE" -in ci/secring.gpg.enc -out ci/secring.gpg || exit 1

    echo
    echo "Build, test and generate Sonar report"
    mvn -B clean org.jacoco:jacoco-maven-plugin:prepare-agent package sonar:sonar || exit 1

    echo
    echo "Build and sign with GPG"
    mvn -B -P release -DskipTests=true -s ci/settings.xml clean verify || exit 1

    echo
    echo "Build output"
    ls -l target */target
else
    mvn -B clean verify
fi
