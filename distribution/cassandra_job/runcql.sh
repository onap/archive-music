#! /bin/bash
if [ $CASS_HOSTNAME ]; then
    echo "#############################################"
    echo "############## Let run cql's ################"
    echo "#############################################"
    echo "Current Variales in play"
    echo "Default User"
    echo "DEF_USER="$DEF_USER
    echo "DEF_PASS=***********"
    echo "New User"
    echo "USERNAME="$USERNAME
    echo "PASSWORD=***********"
    if cqlsh -u cassandra -p cassandra -e "describe keyspaces;";
    then
        >&2 echo "Cassandra user still avalable, will continue as usual";
    else
        if cqlsh -u $USERNAME -p $PASSWORD -e "describe keyspaces;";
        then
            >&2 echo "Password $USERNAME in play, update Variables"
            DEF_USER=$USERNAME
            DEF_PASS=$PASSWORD
            if cqlsh -u $USERNAME -p $PASSWORD -e "describe keyspaces;" | grep admin1;
            then
                >&2 echo "Admin table exists, everything looks good"
                exit 0;
            else
                >&2 echo "Admin does not exists but password has changed. Continue as usual with proper username set"
                >&2 echo "DEF_USER=" $DEF_USER
            fi
        else
            >&2 echo "Continue and as usual"
        fi
    fi
    echo "admin.cql file:"
    cat /cql/admin.cql
    >&2 echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT};
    then
        >&2 echo "Success - admin.cql - Admin keyspace created";
    else
        >&2 echo "Failure - admin.cql";
        exit 0;
    fi
    echo "admin_pw.cql file:"
    cat /cql/admin_pw.cql
    >&2 echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT};
    then
        >&2 echo "Success - admin_pw.cql - Password Changed";
    else
        >&2 echo "Failure - admin_pw.cql";
        exit 0;
    fi

    for f in /cql/extra/*; do
        case "$f" in
            *.cql)
                echo "$0: running $f" && cqlsh -u ${USERNAME} -p ${PASSWORD} -f "$f" ${CASS_HOSTNAME} ${PORT};
                ;;
            *)
                echo "$0: ignoring $f"
                ;;
        esac
    done
else
    >&2 echo "Missing CASS_HOSTNAME";
    exit 0;
fi

