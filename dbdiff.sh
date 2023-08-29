#!/bin/bash
VERSION="1.0.0"

dump_data() {
	host="$1"
	user="$2"
	tb="$4"
	log="$5"
	MYSQL_PWD="$3" mysqldump -h "${host}" -u "${user}" --hex-blob --set-gtid-purged=OFF --single-transaction --order-by-primary --compact --skip-opt "${tb%%.*}" "${tb##*.}" >"${log}/${tb}@${host}.sql"
}

#$1-host1,$2-host2,$3-user,$4-password,$5-db.tb1,db.tb2,db2.tb3
diff_db() {
	tb_list=$(echo "$5" | sed 's/,/\n/g')
	printf "%-38s %-16s %-16s %-10s %s\n" "table" "host1" "host2" "diff_rows" "diff_file"
	for tb in ${tb_list}; do
		dump_data "$1" "$3" "$4" "${tb}" "${log_dir}"
		dump_data "$2" "$3" "$4" "${tb}" "${log_dir}"
		echo "--- db host:$1" >"${log_dir}/${tb}.diff"
		echo "+++ db host:$2" >>"${log_dir}/${tb}.diff"
		if (diff "${log_dir}/${tb}@$1.sql" "${log_dir}/${tb}@$2.sql" >>"${log_dir}/${tb}.diff"); then
			rm -f "${log_dir}/${tb}.diff"
		else
			diff_row=$(grep -E "[<>]" "${log_dir}/${tb}.diff" | wc -l)
			printf "%-38s %-16s %-16s %-10s %s\n" "${tb}" "$1" "$2" "${diff_row}" "${log_dir}/${tb}.diff"
		fi
	done
}

#$1-diff_file file
check_uniq() {
	grep -oP "(?<=VALUES \()\d*" "$1" | uniq -d
}

#$1-host1,$2-host2,$3-diff file
check_conflict() {
	diff_file="$3"
	ids=$(check_uniq "${diff_file}")
	if [ -n "${ids}" ]; then
		true >"${diff_file%.*}.conflict"
		echo "--- db host:$1" >>"${diff_file%.*}.conflict"
		echo "+++ db host:$2" >>"${diff_file%.*}.conflict"
		for id in ${ids}; do
			grep -P ".*VALUES \(${id}[^0-9].*" "${diff_file}" >>"${diff_file%.*}.conflict"
			sed -i "/VALUES (${id},/d" "${diff_file}"
		done
		echo "${diff_file%.*}.conflict"
	fi
}

#$1-tb,$2-diff sql
patch_sql() {
	tb="$1"
	tb="${tb##*.}"
	sql="$2"
	eval table_field=$(echo '$'"${tb^^}_FIELD")
	field_list=$(echo "${table_field}" | sed 's/,/\n/g')
	insert_value=$(echo "${sql}" | grep -oP "(?<=VALUES \().*(?=\))")
	field_idx=$(echo "${field_list}" | grep -n "auto_rotation" | cut -d ':' -f1)
	sql=$(echo "${sql}" | awk -F"," -v N=${field_idx} -v S="'${SYNC_AUTO_ROTATION}'" '{sub($N,S)}1')
	sql2="${sql%*;} ON DUPLICATE KEY UPDATE"
	i=0
	for item in ${field_list}; do
		i=$((i + 1))
		if (echo "${FILTER_FIELD}" | grep -q -w "${item}"); then
			continue
		fi
		sql2="${sql2} ${item}=$(echo "${insert_value}" | cut -d , -f $i),"
	done
	echo "${sql2%*,};"
}
#$1-tb,$2-diff sql
patch_diff() {
	true >"$2.patch"
	while read -r line; do
		patch_sql "$1" "${line}" >>"$2.patch"
	done <"$2"
	mv -f "$2.patch" "$2"
}

#$1-host1,$2-host2,$3-user,$4-passwd,$5-diff file,$6-1:replace into
diff_sql() {
	diff_file="$5"
	if [ -s "${diff_file}" ]; then
		tb=$(basename "${diff_file}" ".${diff_file##*.}")
		tb="${tb%%@*}"
		db="${tb%%.*}"
		if (echo "${FILTER_TABLE}" | grep -q -w "${tb}"); then
			patch_diff "${tb}" "${diff_file}"
		elif [ "$6" = "1" ]; then
			sed -i "s/INSERT INTO/REPLACE INTO/g" "${diff_file}"
		fi
		MYSQL_PWD="$4" mysql -h "$1" -u "$3" "${db}" <"${diff_file}"
		echo "$?"
	else
		rm -f "${diff_file}"
		echo "1"
	fi
}

#$1-host1,$2-host2,$3-user,$4-passwd,$5-diff file,$6-1:replace into
fix_diff() {
	diff_file="$5"
	tb=$(basename "${diff_file}" ".${diff_file##*.}")
	if [ "${FORCE_OVERWRITE}" = "0" ]; then
		conflict=$(check_conflict "$1" "$2" "${diff_file}")
		[ -n "${conflict}" ] && printf "%-38s %-16s %-16s %-12s %s\n" "${tb}" "$1" "$2" "1" "${conflict}"
	fi
	grep "< INSERT INTO" "${diff_file}" | cut -c3- >"${log_dir}/${tb}@$2.fix.sql"
	sync_status=$(diff_sql "$1" "$2" "$3" "$4" "${log_dir}/${tb}@$2.fix.sql" "$6")
	[ "${sync_status}" = "0" ] ||  conflict="$5"
	printf "%-38s %-16s %-16s %-12s %s\n" "${tb}" "$1" "$2" "${sync_status}" "${conflict}"
	if [ "${two_way}" = "1" ]; then
		grep "> INSERT INTO" "${diff_file}" | cut -c3- >"${log_dir}/${tb}@$1.fix.sql"
		sync_status=$(diff_sql "$1" "$2" "$3" "$4" "${log_dir}/${tb}@$1.fix.sql" "$6")
		[ "${sync_status}" = "0" ] ||  conflict="$5"
		printf "%-38s %-16s %-16s %-12s %s\n" "${tb}" "$2" "$1" "${sync_status}" "${conflict}"
	fi
}

#$1-host1,$2-host2,$3-user,$4-passwd,$5-diff file list
sync_diff() {
	diff_list="$5"
	if [ "$#" -lt 5 ]; then
		diff_list=$(find "${log_dir}" -name '*.diff' | sort -u)
	fi
	if [ -z "${diff_list}" ]; then
		echo "Cannot find the difference records, you did not run compare table, or the databases are consistent."
		return
	fi
	printf "%-38s %-16s %-16s %-12s %s\n" "table" "host1" "host2" "sync_status" "conflict_file"
	for diff_file in ${diff_list}; do
		fix_diff "$1" "$2" "$3" "$4" "${diff_file}" "${force}"
	done
}

#$1-cmd list
check_depend() {
	for c in $1; do
		if ! command -v "$c" >/dev/null 2>&1; then
			echo "This tool requires $c, but it is not installed, Aborting."
			exit 127
		fi
	done
}

help() {
	echo "dbdiff v${VERSION}"
	echo "Compare tables row by row and output the differences between them, only supports MySQL."
	echo "Usage: dbdiff [-cfhstv] [--host1 DB_HOST] [--host2 DB_HOST] [--user DB_USER] [--password DB_PASSWORD] [--table DB1.TABLE1,DB2.TABLE2...] [--conf CONF_FILE] [--log LOG_DIR]"
	echo -e "\t\t-c\tcompare tables"
	echo -e "\t\t-f\tforce overwrite of table records (There may be a risk of data loss)"
	echo -e "\t\t-h\tprint help"
	echo -e "\t\t-s\tsync table records based on differences"
	echo -e "\t\t-t\ttwo-way sync"
	echo -e "\t\t-v\tprint version"
}

main() {
	check_depend "mysqldump mysql diff"
	while getopts ":-:cfhstv" opt; do
		case "$opt" in
		c) check=1 ;;
		f) force=1 ;;
		s) sync=1 ;;
		t) two_way=1 ;;
		v)
			echo "${VERSION}"
			exit
			;;
		-) case "${OPTARG}" in
			host1)
				db_host1="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			host2)
				db_host2="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			user)
				db_user="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			password)
				db_passwd="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			table)
				db_table="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			conf)
				conf_file="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			log)
				log_dir="${!OPTIND}"
				OPTIND=$(($OPTIND + 1))
				;;
			*)
				echo "Unknown option --${OPTARG}"
				help
				;;
			esac ;;
		h | ?) help ;;
		esac
	done
	conf_file=${conf_file:-db.conf}
	conf_dir=$(dirname "${conf_file}")
	[ "${conf_dir}" = "." ] && conf_file="$(dirname $0)/${conf_file##*/}"
	source "${conf_file}"
	force=${force:-${FORCE_OVERWRITE}}
	two_way=${two_way:-${TWO_WAY_SYNC}}
	db_host1=${db_host1:-${HOST_1}}
	db_host2=${db_host2:-${HOST_2}}
	user=${user:-${DB_USER}}
	password=${password:-${DB_PASSWD}}
	table=${table:-${TABLE_LIST}}
	log_dir=${log_dir:-${DIFF_LOG}}
	if [ "${check}" = "1" ]; then
		diff_db "${db_host1}" "${db_host2}" "${user}" "${password}" "${table}"
	elif [ "${sync}" = "1" ]; then
		sync_diff "${db_host1}" "${db_host2}" "${user}" "${password}"
	fi
}

main "$@"
