import torch
from transformers import AutoTokenizer, AutoModel

class BERTModule:
    '''
    Attributes:
        model choice:
            0:'bert-base-uncased' 
            1:'bert-large-uncased' 
            2: "allenai/longformer-base-4096"
        tokenizer: pretrained tokenizer
        model: pretrained model
    Methods:
        config(): show configuration of pre-trained model used
        embed(input): generate embedding for given input (string/list of string)
    '''
    def __init__(self, vmodel=0):
        '''
        Input: (vmodel) string indicating the pre-trained model; default is 'bert-base-uncased' 
        '''
        self.model_choice = ['bert-base-uncased', 'bert-large-uncased', "allenai/longformer-base-4096"]
        self.vmodel = vmodel
        # Load pre-trained model tokenizer (vocabulary)
        print('Loading model tokenizer...',end='')
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_choice[self.vmodel])
        print('done')
        # Load pre-trained model (weights)
        print('Loading pre-trained model...',end='')
        self.model = AutoModel.from_pretrained(self.model_choice[self.vmodel],output_hidden_states = True) 
        print('done')

    def config(self):
        print('The configuration of model:')
        print(self.model.config)

    def embed(self,input, sentwise=True):
        '''
        Input: (input) query string or document string
                (sentwise) whether to embed through averaging sentences vectors
        Output: (embedding features) one vector for query or dictionary of vectors for doc
        '''
        #if type(input) == str:
        #    return self.__query_embedding(input)
        #else:
        #    return self.__doc_embedding(input,sentwise)

        # check length
        input = input.strip()
        max_len = self.model.config.max_position_embeddings
        sent_tokens = self.tokenizer(input)['input_ids']
        if len(sent_tokens) <= max_len:
            return self.__sentence_embedding(input).tolist() 
        else:
            return self.__truncated_embedding(sent_tokens,max_len).tolist()


    def __query_embedding(self,query):
        '''    
        Input: (query) a query string
        Output: sentence embedding for query
        '''
        return self.__sentence_embedding(query).tolist()# convert tensor to list

    def __doc_embedding(self,doc,sentwise):
        '''
        Input: (doc) a document file variable/a list of sentence strings ie. iterable with each element being sentence string
                (sentwise) whether to embed through averaging sentences vectors
        Output: (doc_embedding) single vector computed from averaging all sentences embeddings
        '''
        doc_sentVectors = {} # a dictionary of tensor lists containing vectors for sentences in doc
        if sentwise:
            for i,line in enumerate(doc):
                sent_embedding = self.__sentence_embedding(line)
                doc_sentVectors[i]=sent_embedding
        else: # add line to string until reaches max number of tokens specified by the model
            max_len = self.model.config.max_position_embeddings
            sent = ''
            for i,line in enumerate(doc):
                new_sent = sent + line.strip()+' '
                sent_len = len(self.tokenizer(new_sent)['input_ids'])
                if sent_len > max_len:
                    sent_embedding = self.__sentence_embedding(sent) 
                    doc_sentVectors[i]=sent_embedding
                    sent = line.strip()+" "
                else:
                    sent = new_sent
            sent_embedding = self.__sentence_embedding(sent) 
            doc_sentVectors[-1]=sent_embedding
        doc_embedding = sum(doc_sentVectors.values())/len(doc_sentVectors)
        return doc_embedding.tolist() # convert tensor to listg
    
    def __sentence_embedding(self,sentence):
        '''
        Input: (sentence) a sentence string, if end with \n, remove it, should less than max length of number of tokens
        Output: (sent_embedding) an embedding vector of sentence
        '''
        encoded_sent = self.tokenizer(sentence.strip(),return_tensors="pt")
        # tells PyTorch not to construct the compute graph during this forward pass
        with torch.no_grad():
            outputs = self.model(**encoded_sent)
            #hidden_states = outputs[2]
            #hidden_states dim:
            #The layer number (13 layers)
            #The batch number (1 sentence)
            #The word / token number (number of tokens in the sentence)
            #The hidden unit / feature number (number of features for a token)
        #    second2last_layer=hidden_states[-2][0] # use the second to last layer and average all tokens vectors
        #    sent_embedding=torch.mean(second2last_layer, dim=0) # tensor output
            sent_embedding=outputs.pooler_output[0] # tensor output
        return sent_embedding

    def __truncated_embedding(self,sent_tokens,max_len):
        doc_Vectors = []
        while len(sent_tokens) > max_len-2:
            trunc_sent = self.tokenizer.decode(sent_tokens[1:(max_len-3)])
            rest_sent = self.tokenizer.decode(sent_tokens[(max_len-3):-1])
            embedding = self.__sentence_embedding(trunc_sent)
            doc_Vectors.append(embedding)
            sent_tokens = self.tokenizer(rest_sent)['input_ids']
        last_vec = self.__sentence_embedding(rest_sent)
        doc_Vectors.append(last_vec)
        embedding = torch.mean(torch.stack(doc_Vectors), dim=0)# tensor output
        return embedding

    
    
