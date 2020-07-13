from aws_create import parse_config, aws_client, aws_open_redshift_port, check_cluster_creation, aws_resource, persist_cluster


def main():
    parse_config()

    redshift = aws_client('redshift', "us-east-1")

    if check_cluster_creation(redshift):
        print('available')
        ec2 = aws_resource('ec2', 'us-east-1')
        persist_cluster(redshift)
        aws_open_redshift_port(ec2, redshift)
    else:
        print('wait..')


if __name__ == '__main__':
    main()