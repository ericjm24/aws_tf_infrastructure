import logging
import csv
import re
from DataSource2 import Cinemagoer
import s3fs
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

item_itemset_pattern = re.compile(
    r"(?:^|\W)(?i:s|itemset)\W?\s?(\d{1,2})\s?[_\W]?\s?(?i:e|item)\W?\s?(\d{1,3})(\W.*|$)"
)
itemset_only_pattern = re.compile(r"(?:^|\W)(?i:itemset)\s?(\d{1,2})(?:\W|$)")
year_pattern = re.compile(r"(\(\d{4}\)|\[\d{4}\]|\{\d{4}\})")
ia = Cinemagoer()
fs = s3fs.S3FileSystem()

NO_RESULTS = "No Results"


def format_title(title=None, year=None, itemset=None, item=None, item_title=None):
    out = f'"{title}"'
    if item_title:
        out += f" {item_title}"
    if not year and not itemset and not item:
        return out
    out += ", "
    if itemset:
        out += f"itemset {itemset}"
    if item:
        out += f" item {item}"
    if year:
        out += f" ({year})"
    return out


def parse_item_number(title):
    orig_title = title
    title = title.replace("_", " ")
    year, itemset, item, item_title = None, None, None, None
    match = year_pattern.search(title)
    if match:
        year = int(match.group(1)[1:-1])
        if (year < 1900) or (year > datetime.now().year):
            year = None
        else:
            title = year_pattern.sub("", title)
    match = item_itemset_pattern.search(title)
    if match:
        title = item_itemset_pattern.sub("", title)
        itemset = int(match.group(1))
        item = int(match.group(2))
        if match.group(3) and len(match.group(3)) > 1:
            item_title = match.group(3)[1:].strip()
    else:
        match = itemset_only_pattern.search(title)
        if match:
            title = itemset_only_pattern.sub("", title)
            itemset = int(match.group(1))
    title = title.strip()
    logger.info(
        f"Parsed title {orig_title} to {format_title(title, year, itemset, item, item_title)}"
    )
    return title, year, itemset, item, item_title


found_items = {}


def pick_item_from_list(item_list, year, is_tv_show=False):
    if not item_list:
        return NO_RESULTS
    if is_tv_show:
        valid_kinds = ("tv itemset", "item")
    else:
        valid_kinds = ("item", "tv_itemset", "item", "tv_item")
    out = item_list[0]
    for item in item_list:
        if year and (item.data["year"] != year):
            continue
        if item.data["kind"] in valid_kinds:
            if item.getID() in found_items.keys():
                temp = found_items[item.getID()]
            else:
                temp = ia.get_item(item.getID())
                found_items.update({item.getID(): temp})
            votes = int(temp.data.get("votes", 0))
            if (votes > 200) or (temp.data["kind"] == "item" and votes > 50):
                out = temp
                break
    return out


def search_item(title, year, is_tv_show=False):
    if (title, year) in found_items.keys():
        return found_items[(title, year)]
    item = pick_item_from_list(ia.search_item(title), year, is_tv_show=is_tv_show)
    if item == NO_RESULTS and (":" in title or "-" in title):
        new_title = ": ".join(x.strip() for x in re.split(":|-", title)[:-1])
        return search_item(new_title, year, is_tv_show=True)
    found_items.update({(title, year): item})
    return item


def get_DataSource2_ids_from_item(item, itemset, item):
    if not item:
        return None, None
    if item == NO_RESULTS:
        return NO_RESULTS, NO_RESULTS
    if itemset and item and item.data["kind"] == "tv itemset":
        if "items" not in item.current_info:
            ia.update(item, "items")
        try:
            item_id = item["items"][itemset][item].getID()
        except:
            item_id = None
        itemset_id = item.getID()
    elif item.data["kind"] == "tv itemset":
        item_id = None
        itemset_id = item.getID()
    elif item.data["kind"] == "item":
        try:
            itemset_id = item.data["item of"].getID()
        except:
            itemset_id = None
        item_id = item.getID()
    else:
        item_id = item.getID()
        itemset_id = None
    return item_id, itemset_id


def get_DataSource2_id(title):
    title, year, itemset, item, item_title = parse_item_number(title)
    is_tv_show = bool(itemset and item)
    if item_title:
        t = f"{title.strip()} {item_title.strip()}"
        logger.info(f"Searching for tv show item titled {t}")
        item = search_item(t, year, True)
        item_id, itemset_id = get_DataSource2_ids_from_item(item, itemset, item)
        if item_id == NO_RESULTS:
            logger.info(f"No results found. Trying again with itemset name {title}")
            item = search_item(title, year, True)
            item_id, itemset_id = get_DataSource2_ids_from_item(item, itemset, item)
    else:
        logging.info(f"Searching for item titled {title}")
        item = search_item(title, year, is_tv_show)
        item_id, itemset_id = get_DataSource2_ids_from_item(item, itemset, item)
    return item_id, itemset_id


def DataSource2_id_format(id):
    if not id:
        return None
    if id == NO_RESULTS:
        return NO_RESULTS
    return f"tt{id}"


def get_all_DataSource2_ids(titles):
    for title in titles:
        try:
            item, itemset = get_DataSource2_id(title)
            logger.info(
                f"Retrieved DATASOURCE2 IDs for {title}: item_id = {item}, itemset_id = {itemset}"
            )
        except Exception as e:
            logger.warning(f"Failed to retrieve information for item title {title}")
            logger.warning(e)
            item = None
            itemset = None
        yield title, DataSource2_id_format(item), DataSource2_id_format(itemset)


def write_DataSource2_ids(titles, bucket, key_pref):
    target_key = (
        f"{key_pref}/DataSource2_title_matches_{datetime.now().strftime('%y%m%d_%H%M%S')}.csv"
    )
    target_path = f"s3://{bucket}/{target_key}"
    with fs.open(target_path, "wt") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "title_DataSource2_id", "itemset_DataSource2_id"])
        writer.writerows(get_all_DataSource2_ids(titles))
    return target_path


def lambda_handler(event, context):
    titles = event["titles"]
    if type(titles) is str:
        titles = [titles]
    if ("target_bucket" not in event.keys()) or ("target_prefix" not in event.keys()):
        return list(get_all_DataSource2_ids(titles))
    target_bucket = event["target_bucket"].rstrip("/")
    if target_bucket.startswith(("s3://", "s3a://", "s3n://")):
        target_bucket = target_bucket.split("//", 1)[-1]
    target_prefix = event["target_prefix"].strip("/")
    return write_DataSource2_ids(titles, target_bucket, target_prefix)
