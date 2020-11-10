import re
import nltk
import tomotopy as tp
import pyLDAvis
import numpy as np

my_stopwords = nltk.corpus.stopwords.words('english')
word_rooter = nltk.stem.snowball.PorterStemmer(ignore_stopwords=False).stem
my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@'

def clean_tweet(tweet, bigrams=False):
    #tweet = remove_users(tweet)
    tweet = re.sub(r'http\S+', '', tweet)
    tweet = tweet.lower() # lower case
    tweet = re.sub('['+my_punctuation + ']+', ' ', tweet) # strip punctuation
    tweet = re.sub('\s+', ' ', tweet) #remove double spacing
    tweet = re.sub('([0-9]+)', '', tweet) # remove numbers
    tweet = re.sub('rt', '', tweet)  # remove rt
    tweet_token_list = [word for word in tweet.split(' ') if word not in my_stopwords] # remove stopwords
    tweet_token_list = [word_rooter(word) if '#' not in word else word for word in tweet_token_list] # apply word rooter
    if bigrams:
        tweet_token_list = tweet_token_list+[tweet_token_list[i]+'_'+tweet_token_list[i+1]
                                            for i in range(len(tweet_token_list)-1)]
    tweet = ' '.join(tweet_token_list)
    return tweet


def get_cleaned_tweets_list(tweets, limit=2):
    cleaned = [clean_tweet(tweet) for tweet in tweets if tweet.count('$') < limit]
    return cleaned

def get_ttm(tweets):
    mdl = tp.LDAModel(k=20)
    cleaned = get_cleaned_tweets_list(tweets)
    for tweet in cleaned:
        mdl.add_doc(tweet.strip().split())

    for i in range(0, 100, 10):
        mdl.train(10)
        print('Iteration: {}\tLog-likelihood: {}'.format(i, mdl.ll_per_word))

    mdl.summary()
    topic_term_dists = np.stack([mdl.get_topic_word_dist(k) for k in range(mdl.k)])
    doc_topic_dists = np.stack([doc.get_topic_dist() for doc in mdl.docs])
    doc_lengths = np.array([len(doc.words) for doc in mdl.docs])
    vocab = list(mdl.used_vocabs)
    term_frequency = mdl.used_vocab_freq
    prepared_data = pyLDAvis.prepare(
        topic_term_dists,
        doc_topic_dists,
        doc_lengths,
        vocab,
        term_frequency
    )
    pyLDAvis.save_html(prepared_data, 'ldavis.html')