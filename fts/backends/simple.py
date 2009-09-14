"Simple Fts backend"
import os
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.db.models import Q

from fts.backends.base import BaseClass, BaseModel, BaseManager
from fts.models import Word, Index

import unicodedata
from fts.words.stop import FTS_STOPWORDS
try:
    from fts.words.snowball import Stemmer
except ImportError:
    from fts.words.porter import Stemmer

WEIGHTS = {
    'A' : 10,
    'B' : 4,
    'C' : 2,
    'D' : 1
}

class SearchClass(BaseClass):
    def __init__(self, server, params):
        self.backend = 'simple'

class SearchManager(BaseManager):
    def __init__(self, **kwargs):
        super(SearchManager, self).__init__(**kwargs)
        # For autocomplete, generally you'd want:
        #   full_index=True and stem_words=False (full_index implies exact_search)
        # For regular Fulltext search, you'd want:
        #   full_index=False, steam_words=True and exact_search=True
        self.full_index = kwargs.get('full_index', False)
        self.stem_words = kwargs.get('stem_words', True)
        self.exact_search = kwargs.get('exact_search', True)

    def _get_idx_words(self, line, minlen=0):
        words = self._get_words(line, minlen)
        if self.full_index:
            # Find all the substrings of the word
            def substrings(word):
                for i in range(len(word)):
                    for j in range(i+1, len(word)+1):
                        yield word[i:j]
            words = set( perm for word in words for perm in substrings(word) if len(perm) > minlen )
        return words
    
    def _get_words(self, line, minlen=0):
        # Remove accents
        line = ''.join((c for c in unicodedata.normalize('NFD', unicode(line)) if unicodedata.category(c) != 'Mn'))
        # Lowercase and split in a set of words
        words = set(line.lower().split())
        # Stemmer function
        if self.stem_words:
            stem = Stemmer(self.language_code)
        else:
            stem = lambda w: w
        # Get stemmed set of words not in the list of stop words and with a minimum of a minlen length
        return set( stem(word) for word in words if word and word not in FTS_STOPWORDS[self.language_code] and len(word) > minlen )
        
    def _update_index(self, pk):
        if pk is not None:
            if isinstance(pk, (list,tuple)):
                items = self.filter(pk__in=pk)
            else:
                items = self.filter(pk=pk)
            items[0]._index.all().delete()
        else:
            items = self.all()
            ctype = ContentType.objects.get_for_model(self.model)
            Index.objects.filter(content_type__pk=ctype.id).delete()
        
        IW = {}
        for item in items:
            item_words = {}
            for field, weight in self._fields.items():
                if callable(field):
                    words = field(item)
                else:
                    words = item
                    for col in field.split('__'):
                        words = getattr(words, col)
                for word in self._get_idx_words(words):
                    try:
                        iw = IW[word];
                    except KeyError:
                        iw = Word.objects.get_or_create(word=word)[0]
                        IW[word] = iw
                    if ord(weight) < ord(item_words.get(iw, 'Z')):
                        item_words[iw] = weight
            for iw, weight in item_words.items():
                Index.objects.create(content_object=item, word=iw, weight=WEIGHTS[weight])

    def _search(self, query, **kwargs):
        rank_field = kwargs.get('rank_field')
        qs = self.get_query_set()
        
        joins = []
        weights = []
        joins_params = []
        for idx, word in enumerate(self._get_words(query)):
            if self.full_index or self.exact_search:
                joins.append("INNER JOIN %%(words_table_name)s AS w%(idx)d ON (w%(idx)d.word = %%%%s) INNER JOIN %%(index_table_name)s AS i%(idx)d ON (w%(idx)d.id = i%(idx)d.word_id AND i%(idx)d.content_type_id = %%(content_type_id)s AND i%(idx)d.object_id = %%(table_name)s.id)" % { 'idx':idx })
                joins_params.append("'%s'" % word.replace("'", "''"))
            else:
                joins.append("INNER JOIN %%(words_table_name)s AS w%(idx)d ON (w%(idx)d.word LIKE %%%%s) INNER JOIN %%(index_table_name)s AS i%(idx)d ON (w%(idx)d.id = i%(idx)d.word_id AND i%(idx)d.content_type_id = %%(content_type_id)s AND i%(idx)d.object_id = %%(table_name)s.id)" % { 'idx':idx })
                joins_params.append("'%s%%%%'" % word.replace("'", "''"))
                qs.query.distinct = True
            weights.append("i%(idx)d.weight" % { 'idx':idx })
        
        table_name = self.model._meta.db_table
        words_table_name = qs.query.quote_name_unless_alias(Word._meta.db_table)
        index_table_name = qs.query.quote_name_unless_alias(Index._meta.db_table)
        
        ctype = ContentType.objects.get_for_model(self.model)
        joins = ' '.join(joins) % {
            'table_name': qs.query.quote_name_unless_alias(table_name),
            'words_table_name': words_table_name,
            'index_table_name': index_table_name,
            'content_type_id': ctype.id,
        }
        # these params should be set as form params to be returned by get_from_clause() but it doesn't support form params
        joins = joins % tuple(joins_params)
        
        # monkey patch the query set:
        qs.query.table_alias(table_name) # create alias
        qs.query.alias_map[table_name] = (table_name, joins, None, None, None, None, None) # map the joins to the alias
        
        if rank_field is not None:
            select = {}
            order = []
            select[rank_field] = '+'.join(weights)
            order = ['-%s' % rank_field]
            qs = qs.extra(select=select, order_by=order)
        
        return qs

class SearchableModel(BaseModel):
    class Meta:
        abstract = True

    _index = generic.GenericRelation(Index)

    objects = SearchManager()
