(echo -ne "auth 1\n1 sha1 ";dd if=/dev/urandom bs=512 count=1 | openssl md5) > authkeys
cp -f ha.cf /etc/ha.d/
cp -f authkeys /etc/ha.d/
chmod 600 /etc/ha.d/authkeys
chown root.root /etc/ha.d/authkeys
