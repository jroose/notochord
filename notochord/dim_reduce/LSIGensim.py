from .. import App, schema, ABCArgumentGroup, ModelSet, Model, lookup, persist, lookup_or_persist, WorkOrderArgs, filter_widgets
import sqlalchemy
import sys
import uuid

import numpy as np
from gensim.models import LsiModel

class LSIGensimTrainArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--n-components", type=int, action="store", metavar="INT", default=None, help="Number of n_components remaining", required=True)
        group.add_argument("--chunk-size", type=int, action="store", metavar="INT", default=None, help="Size of chunks to process")
        group.add_argument("--output-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of output feature set (required)")
        group.add_argument("--input-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature set (required)")
        group.add_argument("model_name", type=unicode, action="store", metavar="NAME", default=None, nargs='?', help="Name of the model_set")

class LSIGensimTrain(App):
    @staticmethod
    def build_parser_groups():
        return [LSIGensimTrainArgs(), WorkOrderArgs()] + App.build_parser_groups()

    def __init__(self, datadir, input_feature_set=None, output_feature_set=None, min_idwidget=None, max_idwidget=None, datasources=None, chunk_size=None, model_name=None, n_components=None, **kwargs):
        super(LSIGensimTrain, self).__init__(datadir, **kwargs)
        self.config['output_feature_set'] = output_feature_set or self.config['output_feature_set']
        self.config['input_feature_set'] = input_feature_set or self.config['input_feature_set']
        self.config["model_name"] = model_name or self.config.get('model_name', 1024)
        self.config['datasources'] = datasources or self.config.get('datasources')
        self.config['chunk_size'] = chunk_size or self.config.get('chunk_size')
        self.config['min_idwidget'] = (min_idwidget, None)[min_idwidget is None]
        self.config['max_idwidget'] = (max_idwidget, None)[max_idwidget is None]

        self.hyperparameters = {}
        self.hyperparameters['n_components'] = n_components or self.config['hyperparameters']['n_components']

    def main(self):
        from sqlalchemy.sql.expression import select, func
        from ..schema import widget_feature as t_wf
        from ..schema import widget as t_w
        from ..schema import feature as t_f
        from ..schema import feature_set as t_fs
        from ..schema import datasource as t_ds

        with self.session_scope() as session:
            fs_words = session.query(t_fs).filter_by(name=self.config['input_feature_set'])
            fs_lsi = lookup_or_persist(session, t_fs, name=self.config['output_feature_set'], idmodel=None)

            q_words = session.query(t_f.idfeature) \
                .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
                .filter(t_fs.name == self.config['input_feature_set'])

            self.log.info("Getting word decoder ring from database")
            words = dict(enumerate(q_words))
            self.log.info("number of words: {}".format(len(words)))

            self.log.info("Initializing model")
            model_set = ModelSet(session, name=self.config['model_name'])
            n_components = self.hyperparameters['n_components']
            lsi = LsiModel(num_topics=self.hyperparameters['n_components'], id2word=words, onepass=True, extra_samples = n_components + 1, dtype=np.float32)
            self.log.info("Num Topics {}".format(lsi.num_topics))
            model = model_set.new_model(lsi, q_words, params=self.hyperparameters, log=self.log)

            self.log.info("Creating output features")
            fs_lsi = lookup_or_persist(session, t_fs, name=self.config['output_feature_set'], idmodel=model.idmodel)
            output_features = []
            for it in xrange(self.hyperparameters["n_components"]):
                idf = lookup_or_persist(session, t_f, name=unicode(it), idfeature_set=fs_lsi.idfeature_set)
                output_features.append(idf)

            self.log.info("Storing model data")
            q_lsi = session.query(t_f.idfeature) \
                    .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
                    .filter(t_fs.name == self.config['output_feature_set']) \
                    .filter(t_fs.idmodel == model.idmodel)

            model.select_predicts_features(q_lsi)

            all_widgets = session.query(t_w.idwidget)
            all_widgets = filter_widgets(
                    all_widgets,
                    min_idwidget = self.config['min_idwidget'],
                    max_idwidget = self.config['max_idwidget'],
                    datasources = self.config['datasources']
                )

            self.log.info("Counting stuff")
            num_widgets = all_widgets.count()
            self.log.info("Widget count: {}".format(num_widgets))
            num_features = q_words.count()
            self.log.info("Input feature count: {}".format(num_features))
            self.log.info("Output feature count: {}".format(q_lsi.count()))

            chunk_size = self.config['chunk_size']
            for w_t, X in model.get_training_data(all_widgets, sparse_inputs=True, supervised=False, batch_size=chunk_size):
                self.log.info("Training on feature matrix X with shape=({},{}) and nnz={}".format(X.shape[0], X.shape[1], X.nnz))
                lsi.add_documents(X.T)

            model.set_trained(lsi)

            for w_t, X in model.get_predict_data(all_widgets, sparse_inputs=True, supervised=False, batch_size=chunk_size):
                Y_hat = []
                for spdoc in X:
                    doc = zip(spdoc.indices, spdoc.data)
                    spvec = lsi[doc]
                    vec = np.zeros(shape=(lsi.num_topics,), dtype=np.float32)
                    for t, v in spvec:
                        vec[t] = v
                    Y_hat.append(vec)

                    self.log.info("Y_hat[-1] = {}".format(Y_hat[-1]))
                model.update_predictions(w_t, np.array(Y_hat))
        
if __name__ == "__main__":
    LSIGensimTrain.from_args(sys.argv[1:]).run()
