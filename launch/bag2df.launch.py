import os

import yaml
from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def get_params(p):
    with open(p, 'r') as f:
        return yaml.safe_load(f)


def generate_launch_description():
    logger = LaunchConfiguration("log_level")

    config_common_path = LaunchConfiguration('config_common_path')

    config_common = os.path.join(
        get_package_share_directory('bag_utils'),
        'config',
        'config.yaml')

    declare_config_common_path_cmd = DeclareLaunchArgument(
        'config_common_path',
        default_value=config_common,
        description='bag2df configuration')

    bag2df_node = Node(
        package='bag_utils',
        executable='bag2df_node',
        name="bag2df",
        namespace="bag2df",
        output='screen',
        # prefix=['valgrind'],
        # prefix=['xterm -e gdb -ex run --args'],
        # arguments=['--ros-args', '--log-level', logger],
        parameters=[
            config_common_path,
            config_common,
            {

            }
        ])

    # Define LaunchDescription variable and return it
    ld = LaunchDescription()
    ld.add_action(declare_config_common_path_cmd)
    ld.add_action(bag2df_node)
    ld.add_action(DeclareLaunchArgument(
        "log_level",
        default_value=["debug"],
        description="Logging level"))

    return ld
