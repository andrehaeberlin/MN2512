# To learn more about how to use Nix to configure your environment
# see: https://firebase.google.com/docs/studio/customize-workspace
{ pkgs, ... }: {
  # Which nixpkgs channel to use.
  channel = "stable-24.05"; # or "unstable"

  # Use https://search.nixos.org/packages to find packages
  packages = [
    pkgs.python311
    pkgs.python311Packages.pip
    pkgs.python311Packages.virtualenv
    
    # Dependências de sistema cruciais para opencv-python e easyocr
    pkgs.libGL      # Necessário para cv2 (OpenCV)
    pkgs.glib       # Necessário para processamento de imagens
    pkgs.zlib       # Compressão geral
  ];

  # Sets environment variables in the workspace
  env = {
    # Ajuda o pip a encontrar bibliotecas C se necessário
    LD_LIBRARY_PATH = "${pkgs.libGL}/lib:${pkgs.glib.out}/lib";
  };
  
  idx = {
    # Search for the extensions you want on https://open-vsx.org/ and use "publisher.id"
    extensions = [
      "ms-python.python" # Extensão oficial do Python
    ];

    # Enable previews
    previews = {
      enable = true;
      previews = {
        web = {
          # Executa o Streamlit usando o ambiente virtual criado
          command = ["/bin/bash" "-c" "source .venv/bin/activate && streamlit run app.py --server.port $PORT --server.address 0.0.0.0"];
          manager = "web";
          env = {
            # Environment variables to set for your server
            PORT = "$PORT";
          };
        };
      };
    };

    # Workspace lifecycle hooks
    workspace = {
      # Runs when a workspace is first created
      onCreate = {
        # Cria o ambiente virtual e instala as dependências
        install-dependencies = '''
          python -m venv .venv
          source .venv/bin/activate
          pip install --upgrade pip
          pip install torch torchvision --extra-index-url https://download.pytorch.org/whl/cpu
          pip install easyocr
          pip install -r requirements.txt
        ''';
      };
      # Runs when the workspace is (re)started
      onStart = {
        # Garante que o ambiente virtual esteja pronto para uso no terminal
        activate-venv = "source .venv/bin/activate";
      };
    };
  };
}