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
    """
    Functions for dealing with blobs of text.

    """

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
        """
        Concatenates contents of specified files into a target file and saves it.

        :param input_files: List of text files to concatenate.
        :param output_file: Target output file.
        :return: Concatenated target output file.
        """
        with open(output_file, 'w') as outfile:
            for fname in input_files:
                with open(fname) as infile:
                    outfile.write(infile.read())

    @classmethod
    def find_replace_text(cls, directory, find, replace, file_pattern):
        """
        Finds and replaces a string in all text files in a target folder.

        :param directory: Target folder containing files.
        :param find: String to search for.
        :param replace: New string to replace with.
        :param file_pattern: Pattern of text files to scan.
        :return: Saved files with replaced text.
        """
        for path, dirs, files in os.walk(os.path.abspath(directory)):
            for filename in fnmatch.filter(files, file_pattern):
                filepath = os.path.join(path, filename)
                with open(filepath) as f:
                    s = f.read()
                s = s.replace(find, replace)
                with open(filepath, "w") as f:
                    f.write(s)

    @classmethod
    def dump_json(cls, obj, filepath):
        """
        Exports a Python dictionary object to a JSON file.

        :param obj: Python dictionary object.
        :param filepath: Filepath to save object to.
        :return: Exported JSON file.
        """
        with open(filepath, 'w') as outfile:
            json.dump(obj, outfile)

    @classmethod
    def dump_sql(cls, obj, filepath):
        """
        Exports a Python string object to a SQL file.

        :param obj: Python string object.
        :param filepath: Filepath to save object to.
        :return: Exported SQL file.
        """
        with open(filepath, "w") as outfile:
            outfile.write(obj)
        outfile.close()
