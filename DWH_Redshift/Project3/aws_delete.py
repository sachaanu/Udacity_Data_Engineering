#Delete cluster

from aws_create import parse_config, aws_client, check_cluster_creation
from aws_create import aws_resource, destroy_cluster, get_redshift_cluster_status

def main():
    parse_config()

    redshift = aws_client('redshift', "us-east-1")

    if check_cluster_creation(redshift):
        print('available')
        destroy_cluster(redshift)
        print('New redshift cluster status: ')
        print(get_redshift_cluster_status(redshift))
    else:
        print('wait..')


if __name__ == '__main__':
    main()