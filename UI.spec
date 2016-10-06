# -*- mode: python -*-

# pyinstaller --windowed -y --clean --onefile UI.spec

block_cipher = None


a = Analysis(['ui/UI.py'],
             pathex=['/home/niko/Sync/Uni/2016 WS -17/WeBike/scripts'],
             binaries=None,
             datas=[('ui/glade', 'ui/glade')],
             hiddenimports=[],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='UI',
          debug=False,
          strip=False,
          upx=True,
          console=False )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='UI')
