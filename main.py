import sys
from cloudquery.sdk import serve

from cloudquery.sdk.internal.memdb import MemDB


def main():
    p = MemDB()
    serve.PluginCommand(p).run(sys.argv[1:])


if __name__ == "__main__":
    main()
