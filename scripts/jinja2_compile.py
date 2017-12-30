#!/usr/bin/env python

"""cpp/header generator."""

import argparse
import os

from jinja2 import Environment, FileSystemLoader


def main():
    """Main."""
    parser = argparse.ArgumentParser(description='jinja2 compiler')
    parser.add_argument('infile', nargs='+')
    parser.add_argument('--out', '-o', help='Output directory path', required=True)

    args = parser.parse_args()

    dirs = set(os.path.dirname(arg) for arg in args.infile)

    env = Environment(loader=FileSystemLoader(dirs, followlinks=True))
    for filename in args.infile:
        basename = os.path.basename(filename)
        file, ext = os.path.splitext(basename)
        assert(ext == '.jinja')
        root, c_ext = os.path.splitext(file)
        if not c_ext:
            c_ext = '.cc'
        template = env.get_template(basename)
        out_name = os.path.join(args.out, root + '.jinja' + c_ext)
        with open(out_name, 'w') as f:
                f.write(template.render())

if __name__ == "__main__":
    main()
