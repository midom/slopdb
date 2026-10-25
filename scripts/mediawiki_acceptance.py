#!/usr/bin/env python3
"""Exercise an installed, unmodified MediaWiki through HTTP (stdlib only).

This optional acceptance runner requires a running wiki and an administrator
account. It creates pages and an ordinary user, and keeps them for inspection.
Ordinary Go regression tests have no dependency on this runner or on PHP.
"""

import argparse
import datetime
import http.cookiejar
import json
from pathlib import Path
import secrets
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--username", default="Admin")
    parser.add_argument("--password-file", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--title", default="SlopDB acceptance " + datetime.datetime.now(
        datetime.timezone.utc).strftime("%Y%m%d%H%M%S"))
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    steps = []
    report = {"base_url": base, "title": args.title, "steps": steps, "passed": False}

    def redact(value):
        if isinstance(value, dict):
            return {k: ("[redacted]" if "token" in k.lower() or "password" in k.lower()
                        else redact(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value

    def api(name, params, post=False):
        data = urllib.parse.urlencode({"format": "json", "formatversion": 2, **params}).encode()
        request = urllib.request.Request(
            base + "/api.php" + ("" if post else "?" + data.decode()),
            data=data if post else None,
            headers={"User-Agent": "SlopDB-MediaWiki-acceptance/1.0"})
        with opener.open(request, timeout=60) as response:
            result = json.load(response)
        steps.append({"step": name, "response": redact(result)})
        if "error" in result:
            raise AssertionError(name + ": " + json.dumps(redact(result["error"])))
        print(name, "OK", flush=True)
        return result

    def token(kind):
        result = api(kind + "-token", {"action": "query", "meta": "tokens", "type": kind})
        return result["query"]["tokens"][kind + "token"]

    def page(name, title):
        url = base + "/index.php?" + urllib.parse.urlencode({"title": title})
        with opener.open(url, timeout=60) as response:
            html = response.read().decode("utf-8")
            steps.append({"step": name, "status": response.status, "bytes": len(html)})
            assert response.status == 200, name
            assert "mw-exception" not in html and "Wikimedia\\Rdbms\\DBQueryError" not in html, name
        print(name, "OK", flush=True)

    try:
        page("main-page", "Main Page")
        info = api("site-info", {"action": "query", "meta": "siteinfo", "siprop": "general|statistics"})
        report["generator"] = info["query"]["general"]["generator"]
        login = api("login", {"action": "login", "lgname": args.username,
                    "lgpassword": args.password_file.read_text().strip(), "lgtoken": token("login")}, True)
        assert login["login"]["result"] == "Success", "login result"
        csrf = token("csrf")
        title = args.title
        moved = title + " moved"
        text = "A quasar compatibility test. [[Main Page]] [[Category:Compatibility]]"
        edit = api("create-page", {"action": "edit", "title": title, "text": text,
                                  "token": csrf, "watchlist": "watch"}, True)
        assert edit["edit"]["result"] == "Success", "create result"
        edit = api("edit-page", {"action": "edit", "title": title,
                                "appendtext": "\nSecond revision: nebula quasar.", "token": csrf}, True)
        assert edit["edit"]["result"] == "Success", "edit result"
        history = api("history", {"action": "query", "titles": title, "prop": "revisions",
                                  "rvprop": "ids|content", "rvslots": "main", "rvlimit": 5})
        revisions = history["query"]["pages"][0]["revisions"]
        assert len(revisions) == 2 and "Second revision" in revisions[0]["slots"]["main"]["content"], "history content"
        search = api("body-search", {"action": "query", "list": "search", "srsearch": "quasar", "srwhat": "text"})
        assert any(p["title"] == title for p in search["query"]["search"]), "body search omitted page"
        prefix = api("prefix-search", {"action": "query", "list": "prefixsearch", "pssearch": title})
        assert any(p["title"] == title for p in prefix["query"]["prefixsearch"]), "prefix search omitted page"
        cats = api("category-members", {"action": "query", "list": "categorymembers", "cmtitle": "Category:Compatibility", "cmlimit": 500})
        assert any(p["title"] == title for p in cats["query"]["categorymembers"]), "category omitted page"
        changes = api("recent-changes", {"action": "query", "list": "recentchanges", "rclimit": 100})
        assert any(p["title"] == title for p in changes["query"]["recentchanges"]), "recent changes omitted page"
        api("protect-page", {"action": "protect", "title": title, "protections": "edit=sysop", "token": csrf}, True)
        api("move-page", {"action": "move", "from": title, "to": moved, "token": csrf}, True)
        before = api("history-before-delete", {"action": "query", "titles": moved,
                                               "prop": "revisions", "rvlimit": 50})
        expected_revisions = len(before["query"]["pages"][0]["revisions"])
        api("delete-page", {"action": "delete", "title": moved, "token": csrf}, True)
        missing = api("deleted-page", {"action": "query", "titles": moved})
        assert missing["query"]["pages"][0].get("missing"), "page was not deleted"
        restored = api("restore-page", {"action": "undelete", "title": moved, "token": csrf}, True)
        assert restored["undelete"]["revisions"] == expected_revisions, "restore count"
        restored = api("restored-history", {"action": "query", "titles": moved, "prop": "revisions", "rvlimit": 50})
        assert len(restored["query"]["pages"][0]["revisions"]) == expected_revisions, "restored history"
        api("watch-page", {"action": "watch", "titles": moved, "token": token("watch")}, True)
        watch = api("watchlist", {"action": "query", "list": "watchlistraw", "wrlimit": 500})
        assert any(p["title"] == moved for p in watch["watchlistraw"]), "watchlist omitted page"
        users = api("user-list", {"action": "query", "list": "allusers", "auprop": "groups|editcount", "aufrom": args.username})
        admin = next(p for p in users["query"]["allusers"] if p["name"] == args.username)
        assert "sysop" in admin["groups"], "administrator group was corrupted"
        contribs = api("user-contributions", {"action": "query", "list": "usercontribs", "ucuser": args.username, "uclimit": 100})
        assert any(p["title"] == moved for p in contribs["query"]["usercontribs"]), "contributions omitted page"
        links = api("backlinks", {"action": "query", "list": "backlinks", "bltitle": "Main Page", "bllimit": 500})
        assert any(p["title"] == moved for p in links["query"]["backlinks"]), "backlinks omitted page"
        api("block-list", {"action": "query", "list": "blocks"})
        user = "Acceptance" + secrets.token_hex(6)
        password = secrets.token_urlsafe(24)
        created = api("create-account", {"action": "createaccount", "username": user,
                      "password": password, "retype": password, "createreturnurl": base,
                      "createtoken": token("createaccount")}, True)
        assert created["createaccount"]["status"] == "PASS", "create account status"
        for special in ["AllPages", "RecentChanges", "Watchlist", "Preferences", "Statistics", "UserList", "Search"]:
            page("Special:" + special, "Special:" + special)
        api("logout", {"action": "logout", "token": csrf}, True)
        report["passed"] = True
    except Exception as error:
        report["failure"] = str(error)
        raise
    finally:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
