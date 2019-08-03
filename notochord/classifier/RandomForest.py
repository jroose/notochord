from .. import App, schema, ABCArgumentGroup, ModelSet, Model, lookup, persist, CrossvalidationQuery
import sqlalchemy
import sys
import uuid

import numpy as np
import sklearn
from sklearn.ensemble import RandomForestClassifier

class RandomForestTrainArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("model_name", type=unicode, action="store", metavar="NAME", default=None, nargs='?', help="Name of the model_set")

class RandomForestTrain(App):
    @staticmethod
    def build_parser_groups():
        return [RandomForestTrainArgs()] + App.build_parser_groups()

    def __init__(self, datadir, model_name=None, dimensions=None, **kwargs):
        super(RandomForestTrain, self).__init__(datadir, **kwargs)

        self.model_name = model_name or self.config['model_name']
        self.hyperparameters = self.config.get('hyperparameters', {})

    def main(self):
        from sqlalchemy.sql.expression import select, func
        from ..schema import feature_set, feature, widget_feature, widget, model_set

        s = self.get_session()

        model_set = ModelSet(s, name=self.model_name)
        input_features = s.query(feature.idfeature).join(feature_set, feature_set.idfeature_set == feature.idfeature_set).filter_by(name="LSI(words)")
        output_features = s.query(feature.idfeature).join(feature_set, feature_set.idfeature_set == feature.idfeature_set).filter(feature_set.name=="datasource")
        train_widgets = s.query(widget_feature.idwidget) \
            .join(feature, feature.idfeature == widget_feature.idfeature) \
            .join(feature_set, feature_set.idfeature_set == feature.idfeature_set) \
            .filter(feature_set.name=="datasource")

        self.log.info("len(train_widgets):{}".format(train_widgets.count()))

        with CrossvalidationQuery(s, train_widgets, num_splits=5) as CV:
            for train_set, predict_set in CV:
                self.log.info("len(train_set):{} len(predict_set):{}".format(train_set.count(), predict_set.count()))
                clf = RandomForestClassifier(**self.hyperparameters)
                m = model_set.new_model(clf, input_features, output_features)
                for w_t, X_t, Y_t in m.get_training_data(train_set):
                    clf.fit(X_t, Y_t.ravel())

                m.set_trained(clf)

                correct, total = 0, 0
                for w_p, X_p, Y_p in m.get_validation_data(predict_set):
                    Y_hat = clf.predict(X_p)
                    m.update_predictions(w_p, Y_hat)
                    correct += sklearn.metrics.accuracy_score(Y_p, Y_hat, normalize=False)
                    total += len(w_p)

                m.set_metric("Accuracy", float(correct) / float(total))

        s.commit()
        
        
if __name__ == "__main__":
    RandomForestTrain.from_args(sys.argv[1:]).run()
