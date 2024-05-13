start - Does nothing.
add - `<@name> <url> [<iv hash>] [<substitute>]` — Adds new RSS feed to named channel.
update - `<id> <@name> <url> [<iv hash>] [<substitute>]` — Updates given RSS feed, you can change target, feed URL and IV hash.
clean - `<id>` — remove all history from the scraping, next scrape will post all found items
check - `<id>` — check for new items, but all updates will be posted to your chat.
disable - `<id>` — disable feed, no scraping will be done.
enable - `<id>` — enable feed, next scrape will be scheduled.
list - Lists all your created RSS feeds.
delete - `<id>` — Remove RSS feeds and it's history.
