#!/bin/bash -xe

branch=$1
# 去掉 release/ 前缀
version=${branch#release/}
# / 替换成 -
ftp_version=${version//\//-}
echo "branch is ${branch}"
echo "version is ${version}"
echo "ftp_version is ${ftp_version}"

# 重命名
cp target/csp-flink-operator-*.jar target/csp_csp-flink-operator.jar
echo "md5"
md5sum target/csp_csp-flink-operator.jar
if [ "$ftp_version" = "$branch" ]; \
	then \
	echo "will use version: develop"; \
	curl -T target/csp_csp-flink-operator.jar ftp://ftp.4pd.io/pub/pws/hadoop/develop/flink/ ;\
	else \
	echo "will use version: ${ftp_version}"; \
	curl -T target/csp_csp-flink-operator.jar ftp://ftp.4pd.io/pub/pws/hadoop/${ftp_version}/flink/ ;\
fi