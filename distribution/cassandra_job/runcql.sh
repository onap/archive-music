#! /bin/bash
if [ -z "$TIMEOUT" ]; then
    TIMEOUT=10;
fi
TO="--request-timeout=$TIMEOUT"

if [ $CASS_HOSTNAME ]; then
    echo "Sleeping for 60 seconds before running cql";
    sleep 60;
    >&2 echo "#############################################"
    >&2 echo "############## Let run cql's ################"
    >&2 echo "#############################################"
    >&2 echo "Current Variables in play"
    >&2 echo "Default User"
    >&2 echo "DEF_USER="$DEF_USER
    >&2 echo "DEF_PASS=***********"
    >&2 echo "New User"
    >&2 echo "USERNAME="$USERNAME
    >&2 echo "PASSWORD=***********"
    >&2 echo "TIMEOUT="$TIMEOUT
    >&2 echo "Running cqlsh $TO -u cassandra -p cassandra -e \"describe keyspaces;\" ${CASS_HOSTNAME} ${PORT};"
    if cqlsh $TO -u cassandra -p cassandra -e "describe keyspaces;" ${CASS_HOSTNAME} ${PORT};
    then
        >&2 echo "Cassandra user still avalable, will continue as usual";
    else
        CASSFAIL=true
        >&2 echo "$DEF_USER failed, trying with $USERNAME"
        if cqlsh $TO -u $USERNAME -p $PASSWORD -e "describe keyspaces;" ${CASS_HOSTNAME} ${PORT};
        then
            >&2 echo "Password $USERNAME in play, update Variables"
            DEF_USER=$USERNAME
            DEF_PASS=$PASSWORD
            >&2 echo "DEF_USER="$DEF_USER
            >&2 echo "DEF_PASS=***********"
            if cqlsh $TO -u $USERNAME -p $PASSWORD -e "describe keyspaces;" ${CASS_HOSTNAME} ${PORT} | grep admin;
            then
                >&2 echo "Admin table exists, everything looks good"
                exit 0;
            else
                >&2 echo "Admin does not exists but password has changed. Continue as usual with proper username set"
                >&2 echo "DEF_USER=" $DEF_USER
            fi
        else
            if [ $CASSFAIL ]; then
                >&2 echo "$DEF_USER and $USERNAME fail. DB might need to be initialized again. This shouldn't have happend."
                exit 1;
            else
                >&2 echo "Continue and as usual"
            fi
        fi
    fi
    >&2 echo "Running admin.cql file:"
    >&2 echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh $TO -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT};
    then
        >&2 echo "Success - admin.cql - Admin keyspace created";
    else
        >&2 echo "Failure - admin.cql";
        exit 0;
    fi
    >&2 echo "Running admin_pw.cql file:"
    >&2 echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh $TO -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT};
    then
        >&2 echo "Success - admin_pw.cql - Password Changed";
    else
        >&2 echo "Failure - admin_pw.cql";
        exit 0;
    fi

    >&2 echo "Running Test - look for admin keyspace:"
    >&2 echo "Running cqlsh -u $USERNAME -p $PASSWORD -e "select * from system_auth.roles;" ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh $TO -u $USERNAME -p $PASSWORD -e "select * from system_auth.roles;" ${CASS_HOSTNAME} ${PORT}
    then
        >&2 echo "Success - running test";
    else
        >&2 echo "Failure - running test";
        exit 0;
    fi

else
    >&2 echo "Missing CASS_HOSTNAME";
    exit 0;
fi

