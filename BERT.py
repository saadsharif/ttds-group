import torch
from transformers import AutoTokenizer, BertModel

class BERTModule:
    '''
    Attributes:
        tokenizer: pretrained tokenizer see https://huggingface.co/docs/transformers/v4.16.2/en/model_doc/auto#transformers.AutoTokenizer
        model: pretrained BERT model see https://huggingface.co/docs/transformers/v4.16.2/en/model_doc/bert#transformers.BertModel
    Methods:
        config(): show configuration of pre-trained BERT used
        embedding(input): generate embedding for given input (string/list of string)
    '''
    def __init__(self, bert_version='bert-base-uncased'):
        '''
        Input: (bert_version) string indicating the pre-trained version of BERT; default is 'bert-base-uncased' 
        '''
        # Load pre-trained model tokenizer (vocabulary)
        print('Loading BERT tokenizer...',end='')
        self.tokenizer = AutoTokenizer.from_pretrained(bert_version)
        print('done')
        # Load pre-trained model (weights)
        print('Loading pre-trained BERT...',end='')
        self.model = BertModel.from_pretrained(bert_version,output_hidden_states = True) 
        print('done')

    def config(self):
        print('The configuration of BERT model:')
        print(self.model.config)

    def embedding(self,input):
        '''
        Input: (input) query string or document in sentence-based iterable format
        Output: (embedding features) one vector for query or dictionary of vectors for doc
        '''
        if type(input) == str:
            return self.__query_embedding(input)
        else:
            return self.__doc_embedding(input)


    def __query_embedding(self,query):
        '''    
        Input: (query) a query string
        Output: sentence embedding for query
        '''
        return self.__sentence_embedding(query)

    def __doc_embedding(self,doc):
        '''
        Input: (doc) a document file variable/a list of sentence strings ie. iterable with each element being sentence string
        Output: (doc_sentVectors) a dictionary of lists containing vectors for sentences in doc 
        '''
        doc_sentVectors = {}  
        for i,line in enumerate(doc):
            sent_embedding = self.__sentence_embedding(line)
            doc_sentVectors[i]=sent_embedding
        return doc_sentVectors
    
    def __sentence_embedding(self,sentence):
        '''
        Input: (sentence) a sentence string, can end with \n
        Output: (sent_embedding) an embedding vector of sentence
        '''
        encoded_sent = self.tokenizer(sentence,return_tensors="pt")
        # tells PyTorch not to construct the compute graph during this forward pass
        # reduces memory consumption and speeds things up
        with torch.no_grad():
            outputs = self.model(**encoded_sent)
            hidden_states = outputs[2]
            #hidden_states dim:
            #The layer number (13 layers)
            #The batch number (1 sentence)
            #The word / token number (number of tokens in the sentence)
            #The hidden unit / feature number (number of features for a token)
            second2last_layer=hidden_states[-2][0] # use the second to last layer and average all tokens vectors
            sent_embedding=torch.mean(second2last_layer, dim=0).tolist()# convert tensor to list
        return sent_embedding
    
    
