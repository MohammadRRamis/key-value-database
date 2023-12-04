import argparse
from main import put, get, delete, update


def main():
    parser = argparse.ArgumentParser(description="A simple key-value store CLI.")
    subparsers = parser.add_subparsers(dest="command")

    put_parser = subparsers.add_parser(
        "put", help="Put a key-value pair into the database."
    )
    put_parser.add_argument("key", help="The key of the item.")
    put_parser.add_argument("value", help="The value of the item.")

    get_parser = subparsers.add_parser(
        "get", help="Get a value from the database by a key."
    )
    get_parser.add_argument("key", help="The key of the item.")

    delete_parser = subparsers.add_parser(
        "delete", help="Delete a key-value pair from the database by a key."
    )
    delete_parser.add_argument("key", help="The key of the item.")

    update_parser = subparsers.add_parser(
        "update", help="Update a value in the database by a key."
    )
    update_parser.add_argument("key", help="The key of the item.")
    update_parser.add_argument("value", help="The new value of the item.")

    args = parser.parse_args()

    if args.command == "put":
        put(args.key, args.value)
    elif args.command == "get":
        print(get(args.key))
    elif args.command == "delete":
        delete(args.key)
    elif args.command == "update":
        update(args.key, args.value)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
