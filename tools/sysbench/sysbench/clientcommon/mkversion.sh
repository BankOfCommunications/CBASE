
export TARGET=version.ic
export VAR="static const string SIMON_CLIENT_VERSION"
export SIMON_VERSION=$1
export DATE=`date`
export NAME="Simon Client Library"

echo "$VAR = \"$NAME Version $SIMON_VERSION, $DATE\";" > $TARGET
