#!/usr/bin/python

"""
The package_statistics.py is an automation to download and parse a Contents-* file 
in order to retrieve the Top 10 packages with most files associated with it.

First of all, the script can detect if none or more than one arguments were sent
and suggest the right usage of the script. After that, the main function is 
responsible for download the respective Contents file of the argument and call 
the two other functions.

The parseContent is the function that will open the .gz file and parse the text
inside it. The logic behind is that the name of the files associated with each
package is irrelevant. Thus, only the package name is stored and each package count
is what matters for the automation. Analyzing the Contents file, I could notice
that some files were associated with two or more packages. So, a second 'for' loop
was added to parse these packages separated by commas and adding them individually
to the packages list.

As the filenames were not important, the packages were counted to gather the Top 10 
packages with most files associated with it. Python is well know for it's slowness. 
There a number of ways to count each package on a huge million item list, but this 
will result in a slow automation code. The builtins methods it's always faster. The
Count class was a perfect way to count each package. Passing the packages list as 
argument creates a dict subclass where the elements are the dict keys and its values
are their counts.

It's important to notice that at the time this script was run, the Contents-source.gz
file had a format error on the 5742672th and 9420111th line, respectivately
"portable/.AppleDouble/Icon" "unix/.AppleDouble/Icon". These errors makes the file
out of compliance with the syntax rules of Contents-* files, making it impossible
for parsing with the logic used for the other files hence breaking the code. To 
prevent this, the script can caught an IndexError, print the line that isn't 
complying with the rules and continue parse the file.

Not counting the time writing comments and debbuging the Contents-source
error, the total time for writing this automation was about 4 hours.
"""

from collections import Counter
from tqdm import tqdm, trange
import requests
import gzip
import sys

def parseContent(contentFile):
    """Return a list of all packages inside the Contents file
    """

    with gzip.open(contentFile, 'rt') as contents:
        for line in contents:
            try:
                fileAndPkg = line.split()               # Divide the line in two: file and package
                parsedPkgs = fileAndPkg[1].split(',')   # Parse the packages joined together with a comma
                yield from parsedPkgs                   # Loop throught the several packages in the same line
            except IndexError:                          # Try/Except block only for Contents-source.gz. It is
                print(f'Syntax Error: {line}')          # unnecessary if all files obey the syntax rules.


def countPackages(packages):
    """This function is only responsible to get the Top 10 most common packages
    """
    c = Counter(packages)           # Counter object to get the count of each package
    return c.most_common(10)

def main(argv):
    """
    Main entrypoint of the automation script to download the Contents file.
    """

    filename = 'Contents-' + argv + '.gz'
    url = 'http://ftp.uk.debian.org/debian/dists/stable/main/' + filename

    res = requests.get(url, stream=True)
    if res.status_code == 404:      # Prevent a download of a nonexistent file
        sys.exit("Contents file unavailable or nonexistent")
    else:
        size = int(res.headers.get('Content-Length'))

    with open(filename, 'wb') as f, tqdm(total=size, unit='B', unit_scale=True, desc=filename) as bar:
            for chunk in res.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    pkgs = parseContent(filename)
    top10 = countPackages(pkgs)

    print()
    for n, (pkg, count) in enumerate(top10, start=1):
        print(f'{n}. {pkg} \t {count}')

if __name__ == "__main__":
    if (len(sys.argv) == 2):
        main(sys.argv[1])
    else:
        sys.exit('Usage: package_statistics.py {package_name}')
