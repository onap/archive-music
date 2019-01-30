#!/bin/bash
set -e

# first arg is `-f` or `--some-option`
# or there are no args
if [ "$#" -eq 0 ] || [ "${1#-}" != "$1" ]; then
    set -- cassandra -f "$@"
fi

# allow the container to be started with `--user`
if [ "$1" = 'cassandra' -a "$(id -u)" = '0' ]; then
    chown -R cassandra /var/lib/cassandra /var/log/cassandra "$CASSANDRA_CONFIG"
    exec gosu cassandra "$BASH_SOURCE" "$@"
fi

_ip_address() {
    # scrape the first non-localhost IP address of the container
    # in Swarm Mode, we often get two IPs -- the container IP, and the (shared) VIP, and the container IP should always be first
    ip address | awk '
        $1 == "inet" && $NF != "lo" {
            gsub(/\/.+$/, "", $2)
            print $2
            exit
        }
    '
}

if [ "$1" = 'cassandra' ]; then
    : ${CASSANDRA_RPC_ADDRESS='0.0.0.0'}

    : ${CASSANDRA_LISTEN_ADDRESS='auto'}
    if [ "$CASSANDRA_LISTEN_ADDRESS" = 'auto' ]; then
        CASSANDRA_LISTEN_ADDRESS="$(_ip_address)"
    fi

    : ${CASSANDRA_BROADCAST_ADDRESS="$CASSANDRA_LISTEN_ADDRESS"}

    if [ "$CASSANDRA_BROADCAST_ADDRESS" = 'auto' ]; then
        CASSANDRA_BROADCAST_ADDRESS="$(_ip_address)"
    fi
    : ${CASSANDRA_BROADCAST_RPC_ADDRESS:=$CASSANDRA_BROADCAST_ADDRESS}

    if [ -n "${CASSANDRA_NAME:+1}" ]; then
        : ${CASSANDRA_SEEDS:="cassandra"}
    fi
    : ${CASSANDRA_SEEDS:="$CASSANDRA_BROADCAST_ADDRESS"}
    
    sed -ri 's/(- seeds:).*/\1 "'"$CASSANDRA_SEEDS"'"/' "$CASSANDRA_CONFIG/cassandra.yaml"

    for yaml in \
        broadcast_address \
        broadcast_rpc_address \
        cluster_name \
        endpoint_snitch \
        listen_address \
        num_tokens \
        rpc_address \
        start_rpc \
    ; do
        var="CASSANDRA_${yaml^^}"
        val="${!var}"
        if [ "$val" ]; then
            sed -ri 's/^(# )?('"$yaml"':).*/\2 '"$val"'/' "$CASSANDRA_CONFIG/cassandra.yaml"
        fi
    done

    for rackdc in dc rack; do
        var="CASSANDRA_${rackdc^^}"
        val="${!var}"
        if [ "$val" ]; then
            sed -ri 's/^('"$rackdc"'=).*/\1 '"$val"'/' "$CASSANDRA_CONFIG/cassandra-rackdc.properties"
        fi
    done
fi

echo "#############################################"
echo "############## Update music.cql #############"
echo "#############################################"

for f in /docker-entrypoint-initdb.d/a_music.cql; do
    if [ "${MUSIC_REPLICATION_CLASS}" ]; then
        sed -ri 's/REPLICATION_CLASS/'${MUSIC_REPLICATION_CLASS}'/' "$f"
    fi
    if [ "${MUSIC_REPLICATION_FACTOR}" ]; then
        sed -ri 's/REPLICATION_FACTOR/'${MUSIC_REPLICATION_FACTOR}'/' "$f"
    fi
done

echo "#############################################"
echo "######Updating username and password  #######"
echo "#############################################"
for f in /docker-entrypoint-initdb.d/b_pw.cql; do
    if [ "${CASSUSER}" ]; then
        sed -ri 's/CASSUSER/'${CASSUSER}'/' "$f"
    fi
    if [ "${CASSPASS}" ]; then
        sed -ri 's/CASSPASS/'${CASSPASS}'/' "$f"
    fi
done

echo "#############################################"
echo "############## Let run cql's ################"
echo "#############################################"
for f in /docker-entrypoint-initdb.d/*; do
    case "$f" in
        *zzz*.cql)
            echo "$0: running $f" && until $AM && cqlsh -u ${CASSUSER} -p ${CASSPASS} -f "$f"; 
            do >&2 echo "Cassandra is unavailable - sleeping [${f}] $C";let C=C+1; sleep 5; done & ;;
        *a_music.cql)
            echo "$0: running $f" && until $PW && cqlsh -u ${CASSUSER} -p ${CASSPASS} -f "$f" && AM=true; 
            do >&2 echo "Cassandra is unavailable - sleeping [${f}] $D";let D=D+1; sleep 5; done & ;;
        *b_pw.cql)
            echo "$0: running $f" && until cqlsh -u cassandra -p cassandra -f "$f" && PW=true;
            do >&2 echo "Cassandra is unavailable - sleeping [${f}] $E";let E=E+1; sleep 5; done & ;;
        *)        echo "$0: ignoring $f" ;;
    esac

    echo
done


echo "#############################################"
echo "########### Running Password CQL ############"
echo "#############################################"

#echo "$0: running $f" && 
#until cqlsh -u cassandra -p cassandra -f /pw.cql; 
#do >&2 echo "Cassandra is unavailable - sleeping"; sleep 10; done

echo "#############################################"
echo "########### Cassandra Running ###############"
echo "#############################################"


exec "$@"