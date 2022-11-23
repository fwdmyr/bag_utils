import sqlite3
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import deserialize_message
import rclpy
from rclpy.node import Node
import pandas as pd
import functools
import os
import pickle


def rgetattr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def build_tree(obj):
    tree = {}
    build_tree_rec(obj, tree)

    return tree


def build_tree_rec(obj, tree, field_name=''):
    if hasattr(obj, 'get_fields_and_field_types'):
        fields = obj.get_fields_and_field_types()
        for field, field_type in fields.items():
            rec_obj = getattr(obj, field)
            if not field_name:
                rec_field_name = field
            else:
                rec_field_name = field_name + '.' + field
            rec_field_type = type(rgetattr(obj, field))
            if not hasattr(rec_obj, 'get_fields_and_field_types'):
                if isinstance(rec_obj, list) and rec_obj:
                    build_tree_rec(rec_obj[0], tree, 'LIST.' + rec_field_name)
                else:
                    tree[rec_field_name] = rec_field_type
            build_tree_rec(rec_obj, tree, rec_field_name)


def convert_tree(tree):
    temp_tree = {}
    for key, value in tree.items():
        path = key.split('.')
        if path[0] == 'LIST':
            if path[1] not in temp_tree:
                temp_tree[path[1]] = {}
            temp_tree[path[1]]['.'.join(path[2:])] = value
        else:
            temp_tree[key] = value

    return temp_tree


def handle_list(objs, tree):
    rows = [[] for _ in range(len(tree))]
    for obj in objs:
        for idx, key in enumerate(tree.keys()):
            rows[idx].append(rgetattr(obj, key))

    return rows


def build_row(msg, tree):
    row = []
    for key, value in tree.items():
        if isinstance(value, dict):
            list_msg = rgetattr(msg, key)
            sub_tree = value
            row.extend(handle_list(list_msg, sub_tree))
        else:
            obj = rgetattr(msg, key)
            row.append(obj)

    return row


def frame_messages(msgs, tree):
    nested_tree = convert_tree(tree)
    rows = []
    for msg in msgs:
        row = build_row(msg, nested_tree)
        rows.append(row)
    labels = []
    for label in tree.keys():
        if 'LIST' in label:
            label = label[5:] + 's'
        labels.append(label)
    df = pd.DataFrame(rows, columns=labels)

    return df


class BagFileParser(Node):
    def __init__(self):
        super().__init__('bag2df')
        self.declare_parameter('inputDirectory', '')
        self.declare_parameter('outputDirectory', '')
        self.input_directory = self.get_parameter('inputDirectory').value
        self.output_directory = self.get_parameter('outputDirectory').value
        self.get_logger().info(f'Input directory: {self.input_directory}')
        self.get_logger().info(f'Output directory:  {self.output_directory}')

        self.conn = None
        self.cursor = None
        self.topic_type = None
        self.topic_id = None
        self.topic_msg_message = None
        self.topic_names = None

        self.databases = self.find_databases()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def find_databases(self):
        databases = []
        already_processed = []

        for subdir, dirs, files in os.walk(self.output_directory):
            for file in files:
                if file.endswith('.pkl'):
                    _, name = os.path.split(file)
                    name = name.replace('.pkl', '.db3')
                    already_processed.append(name)

        for subdir, dirs, files in os.walk(self.input_directory):
            for file in files:
                if file.endswith('_0.db3') and file not in already_processed:
                #if file.startswith('final-WS60.0-K3-NU1-SL0.5') and file not in already_processed:
                    databases.append(os.path.join(subdir, file))

        return databases

    def connect(self, database):
        _, name = os.path.split(database)
        self.get_logger().info(f'Connecting to database {name}...')
        self.conn = sqlite3.connect(database)
        self.cursor = self.conn.cursor()
        topics_data = self.cursor.execute("SELECT id, name, type FROM topics").fetchall()
        self.topic_type = {name_of: type_of for id_of, name_of, type_of in topics_data}
        self.topic_id = {name_of: id_of for id_of, name_of, type_of in topics_data}
        self.topic_msg_message = {name_of: get_message(type_of) for id_of, name_of, type_of in topics_data}
        self.topic_names = list(self.topic_id.keys())

    def disconnect(self):
        self.conn.close()

    def run(self):
        for database in self.databases:
            dfs = {}

            self.connect(database)

            for name in self.topic_names:
                self.get_logger().info(f'Processing topic {name}...')
                msgs = self.get_messages(name)
                if msgs:
                    tree = build_tree(msgs[0])
                    df = frame_messages(msgs, tree)
                    dfs[name] = df

            self.disconnect()

            _, pkl_file = os.path.split(database)
            pkl_file = pkl_file.replace('.db3', '.pkl')
            self.get_logger().info(f'Writing data to {pkl_file}...')
            with open(self.output_directory + pkl_file, 'wb') as f:
                pickle.dump(dfs, f, pickle.HIGHEST_PROTOCOL)

            self.get_logger().info(f'Done')

    def get_messages(self, topic_name):
        topic_id = self.topic_id[topic_name]

        rows = self.cursor.execute(
            "SELECT timestamp, data FROM messages WHERE topic_id = {}".format(topic_id)).fetchall()

        deserialized_messages = []
        for timestamp, data in rows:
            try:
                msg = deserialize_message(data, self.topic_msg_message[topic_name])
            except:
                pass
            deserialized_messages.append(msg)

        return deserialized_messages


def parse_header(msgs):
    sec = rgetattr(msgs[0], "header.stamp.sec")
    nanosec = rgetattr(msgs[0], "header.stamp.nanosec")
    t_ref = sec + 1E-9 * nanosec

    return [rgetattr(msg, "header.stamp.sec") + 1E-9 * rgetattr(msg, "header.stamp.nanosec") - t_ref for msg in msgs]


def main(args=None):
    rclpy.init(args=args)
    node = BagFileParser()
    node.run()
    node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
