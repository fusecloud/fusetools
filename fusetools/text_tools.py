"""
Tools for performing text tasks.

|pic1|
    .. |pic1| image:: ../images_source/text_tools/text.jpeg
        :width: 25%
"""

import os
import json
import numpy as np
import os, fnmatch


class Blob:

    @classmethod
    def text_parse(cls, blob1, blob2):
        """
        Takes a blob of values input into 'TextArea' box widget and returns a numpy array
        Text blobs can be comma, space or line delimited

        :param blob1:
        :param blob2:
        :return:
        """

        # blob 1 input
        ##if comma separated values
        if "," in blob1:
            p1 = np.asarray(blob1.split(','), dtype=np.float32)
        # #if space separated values
        elif " " in blob1:
            p1 = np.asarray(blob1.split(' '), dtype=np.float32)
        # #if line delimited values
        else:
            blob1 = blob1.replace('\n', ',')
            p1 = np.asarray(blob1.split(','), dtype=np.float32)
        # blob 2 input
        # if comma separated values
        if "," in blob2:
            p2 = np.asarray(blob2.split(','), dtype=np.float32)
        # #if space separated values
        elif " " in blob2:
            p2 = np.asarray(blob2.split(' '), dtype=np.float32)
        # #if line delimited values
        else:
            blob2 = blob2.replace('\n', ',')
            p2 = np.asarray(blob2.split(','), dtype=np.float32)
        return p1, p2


class Export:
    """
    Functions for exporting Python text objects.

    """

    @classmethod
    def concat_text_files(cls, input_files, output_file):
        with open(output_file, 'w') as outfile:
            for fname in input_files:
                with open(fname) as infile:
                    outfile.write(infile.read())

    @classmethod
    def find_replace_text(cls, directory, find, replace, file_pattern):
        for path, dirs, files in os.walk(os.path.abspath(directory)):
            for filename in fnmatch.filter(files, file_pattern):
                filepath = os.path.join(path, filename)
                with open(filepath) as f:
                    s = f.read()
                s = s.replace(find, replace)
                with open(filepath, "w") as f:
                    f.write(s)

    @classmethod
    def dump_json(cls, obj, dir):
        """


        :param obj:
        :param dir:
        :return:
        """
        with open(f'{dir}.json', 'w') as outfile:
            json.dump(obj, outfile)

    @classmethod
    def dump_sql(cls, obj, dir):
        """

        :param obj:
        :param dir:
        :return:
        """
        with open(f"{dir}.sql", "w") as outfile:
            outfile.write(obj)
        outfile.close()
