#!/usr/bin/env bash

set -e
SCRIPT_DIR=$(dirname $0)

USER_GRADLE_PROPERTIES=~/.gradle/gradle.properties

check_configured() {
    grep -q $1 ${USER_GRADLE_PROPERTIES} || echo "$1 not configured in ${USER_GRADLE_PROPERTIES}. $2"
}

check_configuration() {
    if [ ! -f ${USER_GRADLE_PROPERTIES} ]; then
        echo "${USER_GRADLE_PROPERTIES} does not exist"
        exit 1
    fi

    check_configured "sonatypeUsername" "This is the username you use to authenticate with sonatype nexus (e.g. otto-de)"
    check_configured "sonatypePassword" "This is the password you use to authenticate with sonatype nexus (ask Guido or one of the developers)"
    check_configured "signing.secretKeyRingFile" "This is the gpg secret key file, e.g. ~/.gnupg/secring.gpg. If this doesn't exist, generate a key: gpg --gen-key"
    check_configured "signing.keyId" "This is the id of your key (e.g. 72FE5380). Use gpg --list-keys (gpg version >= 2.1: gpg --list-keys --keyid-format short -> use short key with 8 characters) to find yours"
    check_configured "signing.password" "This is the password you defined for your gpg key"
    # gpg --send-keys --keyserver keyserver.ubuntu.com yourKeyId
    # For gpg version >= 2.1: gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg
}

check_configuration

set +e
grep 'version = ".*-SNAPSHOT"' "$SCRIPT_DIR/build.gradle"
NO_SNAPSHOT=$?
set -e

${SCRIPT_DIR}/gradlew -Dorg.gradle.internal.http.socketTimeout=400000 -Dorg.gradle.internal.http.connectionTimeout=400000 publish

if [[ $NO_SNAPSHOT == 1 ]]; then
  echo "Closing and releasing into Sonatype OSS repository"
  "${SCRIPT_DIR}"/gradlew closeAndReleaseRepository
else
  echo "This is a snapshot release, closing in sonatype is not necessary"
fi

