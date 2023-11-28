# Check if MirrorName attribute exists
if ! temporal operator search-attribute list | grep -w MirrorName >/dev/null 2>&1; then
    # If not, create MirrorName attribute
    temporal operator search-attribute create --name MirrorName --type Text
fi

tini -- sleep infinity
