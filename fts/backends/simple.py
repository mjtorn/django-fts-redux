"Simple Fts backend"
import os
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes import generic
from django.db.models import Q

from django.db import transaction

from fts.backends.base import BaseClass, BaseModel, BaseManager
from fts.models import Word, Index

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
    @transaction.commit_on_success
    def update_index(self, pk=None):
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
            for field, weight in self._fields.items():
                for word in set(getattr(item, field).lower().split(' ')):
                    if word and word not in FTS_STOPWORDS[self.language_code]:
                        p = Stemmer(self.language_code)
                        word = p(word)
                        try:
                            iw = IW[word];
                        except KeyError:
                            iw = Word.objects.get_or_create(word=word)[0]
                            IW[w] = iw
                        i = Index(content_object=item, word=iw, weight=WEIGHTS[weight])
                        i.save()

    def search(self, query, **kwargs):
        rank_field = kwargs.get('rank_field')
        qs = self.get_query_set()
        
        joins = []
        weights = []
        joins_params = []
        words = 0
        for word in set(query.lower().split(' ')):
            if word and word not in FTS_STOPWORDS[self.language_code]:
                words += 1
                p = Stemmer(self.language_code)
                word = p(word)
                joins.append("INNER JOIN %%(words_table_name)s AS w%(words)d ON (w%(words)d.word LIKE '%%%%s%%%%%%%%%%%%%%%%') INNER JOIN %%(index_table_name)s AS i%(words)d ON (w%(words)d.id = i%(words)d.word_id AND i%(words)d.content_type_id = %%(content_type_id)s AND i%(words)d.object_id = %%(table_name)s.id)" % { 'words':words })
                weights.append("i%(words)d.weight" % { 'words':words })
                joins_params.append(word)
        
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
