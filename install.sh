#!/bin/sh
#
#  $Id$
#
#  Install Virtuoso Universal Server
#
#  Copyright (C) 2007-2018 OpenLink Software
#
#  The copyright above and this notice must be preserved in all
#  copies of this source code.  The copyright above does not
#  evidence any actual or intended publication of this source code.
#  
#  This is unpublished proprietary trade secret of OpenLink Software.
#  This source code may not be copied, disclosed, distributed, demonstrated
#  or licensed except as authorized by OpenLink Software.
#
#  To learn more about this product, or any other product in our
#  portfolio, please check out our web site at:
#
#      http://www.openlinksw.com
#
#  or contact us at:
#
#      general.information@openlinksw.com
#
#  If you have any technical questions, please contact our support
#  staff at:
#
#      technical.support@openlinksw.com
#


#
#  Set up the script
#
umask 022


#
#  Version info
#
VERSION=8.1
export VERSION

#
#  Installation directory
#
VIRTUOSO_HOME=`pwd`
export VIRTUOSO_HOME


#
#  Setup the PATH
#
PATH="$VIRTUOSO_HOME/bin:.:$PATH"
export PATH


#
#  Test for -n or \c in echo command
#
if (echo "testing\c") | grep c >/dev/null; then
    echo_n=-n
    echo_c=
else
    echo_n=
    echo_c='\c'
fi


#
#  Check whether to use uncompress or gunzip
#
UNZIP=uncompress
SUFFIX=Z
IFS="${IFS=   }"; SAVE_IFS="$IFS"; IFS=":"
for dir in $PATH
  do
  if [ -x $dir/gunzip ]
      then
      UNZIP=$dir/gunzip
      SUFFIX=gz
      break
  fi
done
IFS="$SAVE_IFS"


#
#  Extract packages and run the installation scripts
#
if test -f universal-server.taz
then
    echo ""
    echo "- Extracting Virtuoso Universal Server v$VERSION"
    cat universal-server.taz | $UNZIP -c | tar xf -
    if test $? -ne 0
    then
	echo ""
	echo "***"
 	echo "*** ERROR: could not extract files from package."
        echo "***"
        echo "*** Please correct the above errors before trying installation again"
	echo "***"
	exit 1
    fi

    echo ""
    echo "- Checking where license file should be stored"
    if test -d "$OPLMGR_LICENSE_DIR"
    then
	OPLMGR_LICENSE_DIR="$OPLMGR_LICENSE_DIR"
    elif test -d "/Library/Application Support/OpenLink/Licenses"
    then
	OPLMGR_LICENSE_DIR="/Library/Application Support/OpenLink/Licenses"
    elif test -d /etc/oplmgr
    then
	OPLMGR_LICENSE_DIR="/etc/oplmgr"
    elif mkdir -p /etc/oplmgr 2>/dev/null
    then
	OPLMGR_LICENSE_DIR="/etc/oplmgr"
    else
        OPLMGR_LICENSE_DIR="$VIRTUOSO_HOME/bin/"
    fi
    export OPLMGR_LICENSE_DIR
    echo "Please make sure all licenses are stored in: \"$OPLMGR_LICENSE_DIR\""

    echo ""
    echo "- Checking for initial Virtuoso license"
    if test -f virtuoso.lic 
    then
        if test \! -f "$OPLMGR_LICENSE_DIR/virtuoso.lic"
 	then
	    if cp virtuoso.lic "$OPLMGR_LICENSE_DIR/virtuoso.lic"
	    then
		echo "Virtuoso license stored in: $OPLMGR_LICENSE_DIR"
	    else
		echo "***"
		echo "*** ERROR: could not install virtuoso.lic."
		echo "***"
		echo "*** Please check permissions and install the virtuoso.lic file manually"
		echo "*** into \"$OPLMGR_LICENSE_DIR\""
		echo "***"
	    fi
        fi
    fi

    echo ""
    echo "- Starting OpenLink License Manager"
    install/command-oplmgr.sh start

    echo ""
    echo "- Creating default environment settings"
    install/install-env.sh

    echo ""
    echo "- Creating default database settings"
    install/command-create-db.sh -default-database

    echo ""
    echo "- Registering ODBC drivers"
    install/register-odbc.sh

    echo ""
    echo "- Registering .NET provider for Mono"
    install/register-mono.sh
    
    echo ""
    echo "- Installing VAD packages in database (this can take some time)"
    install/install-vad.sh


    echo ""
    echo "- Starting Virtuoso server instance"
    install/command-startup.sh database

    echo ""
    echo "- Finalizing installation"
    install/install-final.sh

    echo ""
    echo "Installation completed"
else
    echo ""
    echo "***"
    echo "*** ERROR: could not find package to install."
    echo "***"
    exit 1
fi


# ALL DONE
exit 0
