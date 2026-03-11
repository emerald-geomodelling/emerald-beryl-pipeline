"""Tests for GA-LEI model import (Sprint 07 Phase 2)."""

import os
import tempfile
import unittest

import libaarhusxyz
import yaml

import beryl_pipeline.file_import


datadir = os.path.join(os.path.dirname(__file__), "data", "helitem")
GALEI_MODEL_FILE = os.path.join(datadir, "test_galei_model.xyz")
ASC_FILE_GALEI = os.path.join(datadir, "test_helitem_galei.asc")
GEX_FILE_GALEI = os.path.join(datadir, "test_helitem_galei.gex")
ASC_FILE = os.path.join(datadir, "test_helitem.asc")
GEX_FILE = os.path.join(datadir, "test_helitem.gex")

PROJECTION = 32611


def _has_galei_model_data():
    return os.path.exists(GALEI_MODEL_FILE)


def _has_galei_asc_data():
    return os.path.exists(ASC_FILE_GALEI) and os.path.exists(GEX_FILE_GALEI)


def _has_test_data():
    return os.path.exists(ASC_FILE) and os.path.exists(GEX_FILE)


class TestGALEIModelImporter(unittest.TestCase):
    """Test the GALEIModelImporter class."""

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_loads_model(self):
        """Should load a .galei_model.xyz file."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertIsInstance(importer.xyz, libaarhusxyz.XYZ)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_has_resistivity(self):
        """Model should have resistivity in layer_data."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertIn('resistivity', importer.xyz.layer_data)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_has_height(self):
        """Model should have height in layer_data."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertIn('height', importer.xyz.layer_data)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_has_dep_bot(self):
        """Model should have dep_bot in layer_data."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertIn('dep_bot', importer.xyz.layer_data)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_layer_count(self):
        """Model should have 34 layers."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertEqual(importer.n_layers, 34)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_sounding_count(self):
        """Test fixture should have 10 soundings."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        self.assertEqual(importer.n_soundings, 10)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_split_by_line(self):
        """Model should be splittable by flight line."""
        importer = beryl_pipeline.file_import.GALEIModelImporter(GALEI_MODEL_FILE)
        lines = importer.xyz.split_by_line()
        self.assertEqual(len(lines), 2)


class TestDumpModelXYZ(unittest.TestCase):
    """Test the _dump_model_xyz helper function."""

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_produces_xyz(self):
        """Should write .xyz file."""
        model_xyz = libaarhusxyz.XYZ(GALEI_MODEL_FILE, normalize=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            beryl_pipeline.file_import._dump_model_xyz(model_xyz, tmpdir, "galei_model")
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "galei_model.xyz")))

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_produces_msgpack(self):
        """Should write .msgpack file."""
        model_xyz = libaarhusxyz.XYZ(GALEI_MODEL_FILE, normalize=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            beryl_pipeline.file_import._dump_model_xyz(model_xyz, tmpdir, "galei_model")
            self.assertTrue(os.path.exists(os.path.join(tmpdir, "galei_model.msgpack")))

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_produces_summary(self):
        """Should write .summary.yml file."""
        model_xyz = libaarhusxyz.XYZ(GALEI_MODEL_FILE, normalize=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            beryl_pipeline.file_import._dump_model_xyz(model_xyz, tmpdir, "galei_model")
            summary_path = os.path.join(tmpdir, "galei_model.summary.yml")
            self.assertTrue(os.path.exists(summary_path))
            with open(summary_path) as f:
                summary = yaml.load(f, Loader=yaml.SafeLoader)
            self.assertIsInstance(summary, dict)

    @unittest.skipUnless(_has_galei_model_data(), "GA-LEI model test data not available")
    def test_xyz_roundtrip(self):
        """Written XYZ should be reloadable with correct layer_data."""
        model_xyz = libaarhusxyz.XYZ(GALEI_MODEL_FILE, normalize=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            beryl_pipeline.file_import._dump_model_xyz(model_xyz, tmpdir, "galei_model")
            reloaded = libaarhusxyz.XYZ(
                os.path.join(tmpdir, "galei_model.xyz"), normalize=True)
            self.assertIn('resistivity', reloaded.layer_data)
            self.assertIn('height', reloaded.layer_data)
            self.assertEqual(len(reloaded.flightlines), 10)


@unittest.skipUnless(_has_galei_asc_data(), "HeliTEM GA-LEI test data not available")
class TestHeliTEMImporterWithGALEI(unittest.TestCase):
    """Test HeliTEM2LibAarhusImporter captures GA-LEI model."""

    @classmethod
    def setUpClass(cls):
        """Create the importer once (conversion is slow)."""
        cls.importer = beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
            files={"ascfile": ASC_FILE_GALEI, "gexfile": GEX_FILE_GALEI},
            scalefactor=1.0,
            projection=PROJECTION,
        )

    def test_has_galei_model(self):
        """Importer should capture the GA-LEI model."""
        self.assertIsNotNone(self.importer.galei_model_xyz)
        self.assertIsInstance(self.importer.galei_model_xyz, libaarhusxyz.XYZ)

    def test_galei_model_has_resistivity(self):
        """GA-LEI model should have resistivity."""
        self.assertIn('resistivity', self.importer.galei_model_xyz.layer_data)

    def test_galei_model_has_height(self):
        """GA-LEI model should have height."""
        self.assertIn('height', self.importer.galei_model_xyz.layer_data)

    def test_galei_model_layers(self):
        """GA-LEI model should have 34 layers."""
        self.assertEqual(
            self.importer.galei_model_xyz.layer_data['resistivity'].shape[1], 34)

    def test_galei_model_sounding_count(self):
        """GA-LEI model should have same number of soundings as raw data."""
        self.assertEqual(
            len(self.importer.galei_model_xyz.flightlines),
            len(self.importer.xyz.flightlines))

    def test_raw_data_still_works(self):
        """Raw gate data should still be available."""
        self.assertIn('Gate_Ch01', self.importer.xyz.layer_data)

    def test_dump_with_galei_model(self):
        """dump() + _dump_model_xyz should produce all expected files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Main dump
            self.importer.dump(
                xyzfile=os.path.join(tmpdir, 'out.xyz'),
                gexfile=os.path.join(tmpdir, 'out.gex'),
                msgpackfile=os.path.join(tmpdir, 'out.msgpack'),
                summaryfile=os.path.join(tmpdir, 'out.summary.yml'),
                geojsonfile=os.path.join(tmpdir, 'out.geojson'),
            )
            # GA-LEI model dump
            beryl_pipeline.file_import._dump_model_xyz(
                self.importer.galei_model_xyz, tmpdir, "galei_model")

            # All files present
            for ext in ['.xyz', '.msgpack', '.summary.yml']:
                self.assertTrue(
                    os.path.exists(os.path.join(tmpdir, 'galei_model' + ext)),
                    f"Missing galei_model{ext}")

            # Main output also present
            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'out.xyz')))


@unittest.skipUnless(_has_test_data(), "HeliTEM test data not available")
class TestHeliTEMImporterWithoutGALEI(unittest.TestCase):
    """Test HeliTEM2LibAarhusImporter with data lacking GA-LEI columns."""

    @classmethod
    def setUpClass(cls):
        """Create the importer once."""
        cls.importer = beryl_pipeline.file_import.HeliTEM2LibAarhusImporter(
            files={"ascfile": ASC_FILE, "gexfile": GEX_FILE},
            scalefactor=1e-12,
            projection=PROJECTION,
        )

    def test_no_galei_model(self):
        """Importer should have galei_model_xyz = None for non-GA-LEI data."""
        self.assertIsNone(self.importer.galei_model_xyz)


if __name__ == '__main__':
    unittest.main()
