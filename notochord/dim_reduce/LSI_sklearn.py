from .. import App, schema, ABCArgumentGroup, Model, lookup, persist, lookup_or_persist, WorkOrderArgs, filter_widgets
import sqlalchemy
import sys
import uuid

import numpy as np
import sklearn.decomposition

class LSITrainArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--n-components", type=int, action="store", metavar="INT", default=None, help="Number of n_components remaining")
        group.add_argument("--output-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of output feature set (required)")
        group.add_argument("--input-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature set (required)")
        group.add_argument("model_name", type=unicode, action="store", metavar="NAME", default=None, nargs='?', help="Name of the model_set")

class LSITrain(App):
    @staticmethod
    def build_parser_groups():
        return [LSITrainArgs(), WorkOrderArgs()] + App.build_parser_groups()

    def __init__(self, datadir, input_feature_set=None, output_feature_set=None, min_idwidget=None, max_idwidget=None, datasources=None, chunk_size=None, model_name=None, n_components=None, **kwargs):
        super(LSITrain, self).__init__(datadir, **kwargs)
        self.config['output_feature_set'] = output_feature_set or self.config['output_feature_set']
        self.config['input_feature_set'] = input_feature_set or self.config['input_feature_set']
        self.config["model_name"] = model_name or self.config.get('model_name')
        self.config['datasources'] = datasources or self.config.get('datasources')
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
            self.log.info("Querying for input features")
            fs_words = session.query(t_fs).filter_by(name=self.config['input_feature_set'])

            self.log.info("Creating output features")
            fs_lsi = lookup_or_persist(session, t_fs, name=self.config['output_feature_set'], idmodel=None)
            output_features = []
            for it in xrange(self.hyperparameters["n_components"]):
                idf = lookup_or_persist(session, t_f, name=unicode(it), idfeature_set=fs_lsi.idfeature_set)
                output_features.append(idf)

            self.log.info("Initializing model")
            model_set = ModelSet(session, name=self.config['model_name'])
            svd = sklearn.decomposition.TruncatedSVD(**self.hyperparameters)

            self.log.info("Querying input matrix")
            q_words = session.query(t_wf.idfeature) \
                    .join(t_w, t_w.idwidget == t_wf.idwidget) \
                    .join(t_f, t_f.idfeature == t_wf.idfeature) \
                    .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
                    .filter(t_fs.name == self.config['input_feature_set']) \
                    .order_by(t_wf.idfeature) \
                    .distinct()
            
            q_words = filter_widgets(
                    q_words,
                    min_idwidget = self.config['min_idwidget'],
                    max_idwidget = self.config['max_idwidget'],
                    datasources = self.config['datasources']
                )

            model = model_set.new_model(svd, q_words, log=self.log)

            self.log.info("Creating output feature set associated with model")
            fs_lsi = lookup_or_persist(session, t_fs, name=self.config['output_feature_set'], idmodel=model.idmodel)
            output_features = []
            for it in xrange(self.hyperparameters["n_components"]):
                idf = lookup_or_persist(session, t_f, name=unicode(it), idfeature_set=fs_lsi.idfeature_set)
                output_features.append(idf)

            self.log.info("Immortalizing prediction features")
            q_lsi = session.query(t_f.idfeature) \
                    .join(t_fs, t_fs.idfeature_set == t_f.idfeature_set) \
                    .filter(t_fs.name == self.config['output_feature_set']) \
                    .filter(t_fs.idmodel == model.idmodel)

            model.select_predicts_features(q_lsi)

            self.log.info("Gathering selection of widgets")
            all_widgets = session.query(t_w.idwidget)
            all_widgets = filter_widgets(
                    all_widgets,
                    min_idwidget = self.config['min_idwidget'],
                    max_idwidget = self.config['max_idwidget'],
                    datasources = self.config['datasources']
                )

            self.log.info("Widget count: {}".format(all_widgets.count()))
            self.log.info("Input feature count: {}".format(q_words.count()))
            self.log.info("Output feature count: {}".format(q_lsi.count()))

            for w_t, X in model.get_training_data(all_widgets, sparse_inputs=True, supervised=False):
                self.log.info("Training on feature matrix X with shape=({},{}) and nnz={}".format(X.shape[0], X.shape[1], X.nnz))
                Y_hat = svd.fit_transform(X)
                model.set_trained(svd)
                model.update_predictions(w_t, Y_hat)
        
if __name__ == "__main__":
    LSITrain.from_args(sys.argv[1:]).run()
