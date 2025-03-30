import pandas as pd

# Input and Output utility class
class InputOutput(object):

    # loads text file into a list
    ''' Input output utility class
    '''
    def load_text(filename, enc='utf-8'):
        text_file = open(filename, "r", encoding=enc)
        no_str = text_file.read()
        text_file.close()
        # make a list
        lines = no_str.split("\n")
        return lines

    # loads numbers from a text file into list
    def load_nums(filename):
        text_file = open(filename, "r")
        no_str = text_file.read()
        text_file.close()
        # make a list
        lines = no_str.split("\n")
        return list(map(int, lines))

    # saves a list to a text file
    def save_text(filename,listname, enc='utf-8'):
        with open(filename, mode='wt', encoding=enc) as myfile:
            myfile.write('\n'.join(listname))

    # loads column of a csv file
    def load_csv_col(infile, colname):
        df = pd.read_csv(infile)
        lines = df[colname]
        return lines.tolist()