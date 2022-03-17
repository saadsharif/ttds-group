def load_stop_words(filename):
    stop_words = []
    with open(filename, 'r') as stop_word_file:
        for line in stop_word_file:
            stop_words.append(line.strip().lower())
    return stop_words


def valid_term(term):
    if ":" in term:
        return False
    return term.isalpha()
