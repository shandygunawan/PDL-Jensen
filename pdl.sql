PGDMP         3                w            pdl    11.4    11.4     �
           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                       false            �
           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                       false            �
           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                       false            �
           1262    24594    pdl    DATABASE     �   CREATE DATABASE pdl WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252';
    DROP DATABASE pdl;
             postgres    false            �            1259    24595    property_ownership    TABLE     �   CREATE TABLE public.property_ownership (
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    property_number integer NOT NULL,
    "Vs" integer NOT NULL,
    "Ve" integer NOT NULL,
    "T" integer NOT NULL,
    "Op" "char" NOT NULL
);
 &   DROP TABLE public.property_ownership;
       public         postgres    false            �
          0    24595    property_ownership 
   TABLE DATA               p   COPY public.property_ownership (customer_id, customer_name, property_number, "Vs", "Ve", "T", "Op") FROM stdin;
    public       postgres    false    196   �       �
   x   x�341�t-KT��L�)N��47�4�44���e�W�)�N�& yO.#s΀Ԓ�"$yS�	T ����,�ە@7�r�1T�v��.ݦPi��9��㍠�\1z\\\ �,YP     