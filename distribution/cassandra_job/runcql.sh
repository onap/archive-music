#! /bin/bash
if [ $CASS_HOSTNAME ]; then
    echo "#############################################"
    echo "############## Let run cql's ################"
    echo "#############################################"
    echo "admin.cql file:"
    cat /cql/admin.cql
    echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin.cql ${CASS_HOSTNAME} ${PORT}; then echo "success"; else echo "failure" && exit 1;fi
    echo "admin_pw.cql file:"
    cat /cql/admin_pw.cql
    echo "Running cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT}"
    sleep 1;
    if cqlsh -u $DEF_USER -p $DEF_PASS -f /cql/admin_pw.cql ${CASS_HOSTNAME} ${PORT}; then echo "success"; else echo "failure" && exit 1;fi

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
    echo "Missing CASS_HOSTNAME";
    exit 1;
fi

