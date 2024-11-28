import argparse
import datetime
import os
import sys
from datetime import timedelta

from azure.storage.blob import (
    BlobServiceClient,
    ContainerSasPermissions,
    generate_container_sas,
)


def generate_container_sas_token(
    blob_service_client: BlobServiceClient,
    container_name: str,
    expiration: timedelta,
    *,
    read: bool = False,
    write: bool = False,
    delete: bool = False,
    list: bool = False,
    delete_previous_version: bool = False,
    tag: bool = False,
    add: bool = False,
    create: bool = False,
):
    now = (
        datetime.datetime.now(datetime.timezone.utc)
        if sys.version_info <= (3, 11)
        else datetime.datetime.now(datetime.UTC)
    )
    return generate_container_sas(
        account_name=blob_service_client.account_name,
        container_name=container_name,
        account_key=blob_service_client.credential.account_key,
        permission=ContainerSasPermissions(
            read=read,
            write=write,
            delete=delete,
            list=list,
            delete_previous_version=delete_previous_version,
            tag=tag,
            add=add,
            create=create,
        ),
        expiry=now + expiration,
        start=now,
    )


def main():
    parser = argparse.ArgumentParser(
        prog="gcst",
        description="Generate container SAS Token",
    )

    parser.add_argument(
        "-C",
        "--connection-string",
        help="set Azure Storage connection string, if not offered the environment variable AZURE_STORAGE_CONNECTION_STRING will be used",
    )
    parser.add_argument(
        "--container-name",
        required=True,
        help="set Azure Storage container name",
    )
    parser.add_argument(
        "-r",
        "--read",
        action="store_true",
        help="set read permission for token",
    )
    parser.add_argument(
        "-w",
        "--write",
        action="store_true",
        help="set write permission for token",
    )
    parser.add_argument(
        "-d",
        "--delete",
        action="store_true",
        help="set delete permission for token",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="set list permission for token",
    )
    parser.add_argument(
        "-t", "--tag", action="store_true", help="set tag permission for token"
    )
    parser.add_argument(
        "-a", "--add", action="store_true", help="set add permission for token"
    )
    parser.add_argument(
        "-c",
        "--create",
        action="store_true",
        help="set create permission for token",
    )

    expiration_group = parser.add_argument_group(
        "token expiration time",
        "expiration time is the combination of days, hours, and minutes. If none of the parameters are informed, the expiration time is 15 minutes",
    )
    expiration_group.add_argument(
        "--days",
        type=int,
        default=0,
        help="set days for token expiration, 0 by default",
    )
    expiration_group.add_argument(
        "--hours",
        type=int,
        default=0,
        help="set hours for token expiration, 0 by default",
    )
    expiration_group.add_argument(
        "--minutes",
        type=int,
        default=0,
        help="set minutes for token expiration, 0 by default",
    )

    display_group = parser.add_argument_group(
        "display generated token",
        "SAS Token ou URL with SAS Token, but not both. If none of the parameters are informed, URL will be displayed",
    )
    display_group_options = display_group.add_mutually_exclusive_group()
    display_group_options.add_argument(
        "--token", action="store_true", help="display SAS Token"
    )
    display_group_options.add_argument(
        "--url", action="store_true", help="display URL"
    )

    parser.add_argument(
        "--version", action="version", version="%(prog)s 0.1.0"
    )

    args = parser.parse_args()

    expiration = (
        timedelta(minutes=15)
        if args.days == args.hours == args.minutes == 0
        else timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
    )

    contains_permission_parameters = (
        args.read
        or args.write
        or args.delete
        or args.list
        or args.tag
        or args.add
        or args.create
    )
    if not contains_permission_parameters:
        parser.exit(
            status=1, message="Exiting because permissions were not provided\n"
        )

    connection_string = args.connection_string or os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING"
    )

    if not connection_string:
        parser.exit(
            status=1,
            message="Exiting because connection string were not provided\n",
        )

    try:
        blob_service_client = BlobServiceClient.from_connection_string(
            conn_str=connection_string
        )
    except ValueError as e:
        parser.exit(status=1, message=f"Error: {e}\n")

    sas_token = generate_container_sas_token(
        blob_service_client,
        args.container_name,
        expiration,
        read=args.read,
        write=args.write,
        delete=args.delete,
        list=args.list,
        tag=args.tag,
        add=args.add,
        create=args.create,
    )

    if args.token:
        print(sas_token)
        exit(0)

    sas_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{args.container_name}?{sas_token}"
    print(sas_url)


if __name__ == "__main__":
    main()
