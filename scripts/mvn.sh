#!/usr/bin/env bash
# Maven launcher that works around the broken /e/ -> \e\ path conversion in Git Bash.
#
# The system `mvn` script hands a POSIX /e/... classpath to the Windows build of `java`, which then
# fails to load org.codehaus.plexus.classworlds.launcher.Launcher. Launching the Launcher directly
# with Windows-style paths avoids that. This lets the KRPC test suite run without a global Maven fix.
set -e

MAVEN_HOME='E:/coding/apache-maven-3.9.14'
BOOT="$MAVEN_HOME/boot/plexus-classworlds-2.9.0.jar"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Convert the POSIX project path into a Windows path the JVM can consume.
if command -v cygpath >/dev/null 2>&1; then
  PROJECT_DIR="$(cygpath -w "$PROJECT_DIR")"
fi

exec java -classpath "$BOOT" \
  "-Dclassworlds.conf=$MAVEN_HOME/bin/m2.conf" \
  "-Dmaven.home=$MAVEN_HOME" \
  "-Dmaven.multiModuleProjectDirectory=$PROJECT_DIR" \
  org.codehaus.plexus.classworlds.launcher.Launcher "$@"
