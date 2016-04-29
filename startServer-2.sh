#!/bin/bash
#
# This script is used to start the server from a supplied config file
#
#exit

#cd ${SVR_HOME}

JAVA_MAIN='gash.router.server.MessageApp'
JAVA_ARGS="runtime/route-2.conf"
echo -e "\n** config: ${JAVA_ARGS} **\n"

# superceded by http://www.oracle.com/technetwork/java/tuning-139912.html
echo ""

javac src/gash/router/server/MessageApp.java
echo "Compiled successfully"
java ${JAVA_MAIN} ${JAVA_ARGS}
