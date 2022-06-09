# https://janikvonrotz.ch/2020/05/07/bulk-download-papers-from-scihub-for-text-mining/
# run this from the root dir with `bash scripts/scihub-download.sh`

urlencode() {
    old_lc_collate=$LC_COLLATE
    LC_COLLATE=C
    local length="${#1}"
    for (( i = 0; i < length; i++ )); do
        local c="${1:i:1}"
        case $c in
            [a-zA-Z0-9.~_-]) printf "$c" ;;
            *) printf '%%%02X' "'$c" ;;
        esac
    done
    LC_COLLATE=$old_lc_collate
}
source .env
readarray -t list < data/doi-list.txt
scihub_url="https://sci-hub.hkvisa.net/"
#scihub_url="https://sci-hub.ee"
for doi in "${list[@]}"
do
    [[ "$doi" == "" ]] && continue
    [[ $doi =~ ^#.* ]] && continue # invalid DOIs can be commented out with '#'
    dir="data/$(echo "$doi" | sed 's/\/.*//')"
    file="$dir/${doi/\//_}.pdf" # create valid file path
    [[ -f $file ]] && continue # do not re-download file if it exists
    [[ -d $dir ]] || mkdir -p $dir
    link=$(curl -s -L $scihub_url --compressed \
        -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:75.0) Gecko/20100101 Firefox/75.0' \
        -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' \
        -H 'Accept-Language: en-US,en;q=0.5' \
        -H 'Content-Type: application/x-www-form-urlencoded' \
        -H 'Origin: $scihub_url' \
        -H 'DNT: 1' \
        -H 'Connection: keep-alive' \
        -H 'Referer: $scihub_url' \
        -H 'Cookie: __ddg1=SFEVzNPdQpdmIWBwzsBq; session=45c4aaad919298b2eb754b6dd84ceb2d; refresh=1588795770.5886; __ddg2=6iYsE2844PoxLmj7' \
        -H 'Upgrade-Insecure-Requests: 1' \
        -H 'Pragma: no-cache' \
        -H 'Cache-Control: no-cache' \
        -H 'TE: Trailers' \
        --data "sci-hub-plugin-check=&request=$(urlencode $doi)". | grep -oP  "(?<=//).+(?=#)")
    if [[ "$link" != "" ]] ; then
      echo "Downloading $doi from $link"
      curl -s -L $link --output $file
    else
      link="${DOI_RESOLVER_URL_PREFIX}$doi"
      echo "You need to manually download $doi from $link"
    fi
done
